#!/usr/bin/env python3
"""
mk20_db_importer.py

DB-safe Curio MK20 DDO importer for SP/operator-side enqueue.

Safety model:
  - Default is no production insert.
  - Production insert requires --execute and --ack-db-direct.
  - Full batch additionally requires --allow-full-batch.
  - Stage excludes anything already present in current Curio DB by deal id, piece CID, allocation id,
    waiting queue, download pipeline, mk20 pipeline, and sectors_sdr_initial_pieces.
  - Insert SQL rechecks conflicts at insert time.
  - Each production insert has a run_id and writes an audit manifest.
  - Rollback and verify SQL are generated for that run_id.

Important: deal_client is the MK20 Deal.Client / market_mk20_deal.client value, normally the f1 wallet
that sptool used as --wallet. client_id is the numeric DataCap allocation client actor id.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import posixpath
import shlex
import subprocess
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

CROCKFORD32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
VERSION = "2026-06-27-public-safe"
EXPECTED_CSV_COLUMNS = ["data_cid", "piece_cid_v1", "pcidv2", "piece_size", "car_size", "car_url"]


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def sql_ident(name: str) -> str:
    if not name.replace("_", "").isalnum() or name[0].isdigit():
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def ulid_from_time_and_key(ms: int, key: str) -> str:
    if ms < 0 or ms >= 2**48:
        raise ValueError("ULID timestamp must fit 48 bits")
    entropy = hashlib.sha256(key.encode("utf-8")).digest()[:10]
    data = ms.to_bytes(6, "big") + entropy
    value = int.from_bytes(data, "big")
    chars = []
    for _ in range(26):
        chars.append(CROCKFORD32[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def norm_piececid(value: Any) -> str:
    if isinstance(value, dict) and "/" in value:
        return str(value["/"]).strip()
    if value is None:
        return ""
    return str(value).strip()


def get_any(d: Dict[str, Any], names: Iterable[str], default: Any = None) -> Any:
    lowered = {str(k).lower(): v for k, v in d.items()}
    for name in names:
        if name in d:
            return d[name]
        if name.lower() in lowered:
            return lowered[name.lower()]
    return default


@dataclass
class Allocation:
    allocation_id: int
    client: int
    miner: int
    piece_cid: str
    piece_size: int
    term_min: int
    term_max: int
    expiration: int
    raw: Dict[str, Any] = field(repr=False)


@dataclass
class Candidate:
    csv_row_no: int
    data_cid: str
    piece_cid_v1: str
    piece_cid_v2: str
    piece_size: int
    car_size: int
    car_url: str
    deal_id: str
    allocation_id: Optional[int] = None
    alloc_term_min: Optional[int] = None
    alloc_term_max: Optional[int] = None
    alloc_expiration: Optional[int] = None
    file_reject_reason: Optional[str] = None

    def data_json(self) -> str:
        return json.dumps(
            {
                "format": {"car": {}},
                "piece_cid": {"/": self.piece_cid_v2},
                "source_http": {
                    "urls": [
                        {"url": self.car_url, "headers": None, "fallback": True, "priority": 0}
                    ]
                },
            },
            separators=(",", ":"),
            sort_keys=False,
        )

    def ddo_v1_json(self, provider: str, duration: int) -> str:
        return json.dumps(
            {
                "ddo": {
                    "duration": duration,
                    "provider": provider,
                    "start_epoch": None,
                    "allocation_id": self.allocation_id,
                    "market_address": "",
                    "market_deal_id": None,
                    "notification_address": "",
                },
                "deal_id": 0,
                "complete": False,
                "error": "",
            },
            separators=(",", ":"),
            sort_keys=False,
        )

    def retrieval_v1_json(self) -> str:
        return json.dumps(
            {"indexing": True, "announce_piece": False, "announce_payload": True},
            separators=(",", ":"),
            sort_keys=False,
        )


def read_allocations(path: Path) -> List[Allocation]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if isinstance(obj, dict):
        records = list(obj.values())
    elif isinstance(obj, list):
        records = obj
    else:
        raise ValueError(f"Unsupported allocations JSON root type: {type(obj).__name__}")

    out: List[Allocation] = []
    for idx, rec in enumerate(records, start=1):
        if not isinstance(rec, dict):
            raise ValueError(f"allocation record #{idx} is not an object")
        aid = get_any(rec, ["allocationid", "allocation_id", "ID", "id"])
        client = get_any(rec, ["client", "Client"])
        miner = get_any(rec, ["miner", "provider", "Provider"])
        piececid = get_any(rec, ["piececid", "piece_cid", "Data", "data"])
        piecesize = get_any(rec, ["piecesize", "piece_size", "Size", "size"])
        termmax = get_any(rec, ["termmax", "term_max", "TermMax"])
        termmin = get_any(rec, ["termmin", "term_min", "TermMin"])
        expiration = get_any(rec, ["expiration", "Expiration"])
        piece_cid = norm_piececid(piececid)
        try:
            out.append(
                Allocation(
                    allocation_id=int(aid),
                    client=int(client),
                    miner=int(miner),
                    piece_cid=piece_cid,
                    piece_size=int(piecesize),
                    term_min=int(termmin),
                    term_max=int(termmax),
                    expiration=int(expiration),
                    raw=rec,
                )
            )
        except Exception as exc:
            raise ValueError(f"bad allocation record #{idx}: {exc}; record={rec!r}") from exc
    return out


def car_url_filename_matches(piece_cid_v1: str, car_url: str) -> bool:
    parsed = urlparse(car_url)
    base = posixpath.basename(parsed.path)
    return base == f"{piece_cid_v1}.car"


def read_csv_candidates(
    path: Path,
    batch_name: str,
    id_time_ms: int,
    allocations_by_piece: Dict[str, List[Allocation]],
    client_id: int,
    provider_id: int,
    piece_size_expected: int,
    duration: int,
) -> List[Candidate]:
    out: List[Candidate] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != EXPECTED_CSV_COLUMNS:
            raise ValueError(f"CSV columns mismatch: got {reader.fieldnames}, expected {EXPECTED_CSV_COLUMNS}")
        for i, row in enumerate(reader, start=2):
            reasons: List[str] = []
            piece_cid_v1 = (row.get("piece_cid_v1") or "").strip()
            piece_cid_v2 = (row.get("pcidv2") or "").strip()
            data_cid = (row.get("data_cid") or "").strip()
            car_url = (row.get("car_url") or "").strip()
            try:
                piece_size = int(row.get("piece_size") or "")
            except Exception:
                piece_size = -1
                reasons.append("invalid piece_size")
            try:
                car_size = int(row.get("car_size") or "")
            except Exception:
                car_size = -1
                reasons.append("invalid car_size")

            if not data_cid:
                reasons.append("missing data_cid")
            if not piece_cid_v1:
                reasons.append("missing piece_cid_v1")
            if not piece_cid_v2:
                reasons.append("missing pcidv2")
            if not car_url:
                reasons.append("missing car_url")
            if piece_size != piece_size_expected:
                reasons.append(f"csv piece_size mismatch: {piece_size} != {piece_size_expected}")
            if car_url and piece_cid_v1 and not car_url_filename_matches(piece_cid_v1, car_url):
                reasons.append("car_url basename does not equal <piece_cid_v1>.car")

            key = f"{batch_name}:{i}:{piece_cid_v1}:{piece_cid_v2}"
            c = Candidate(
                csv_row_no=i,
                data_cid=data_cid,
                piece_cid_v1=piece_cid_v1,
                piece_cid_v2=piece_cid_v2,
                piece_size=piece_size,
                car_size=car_size,
                car_url=car_url,
                deal_id=ulid_from_time_and_key(id_time_ms, key),
            )

            allocs = allocations_by_piece.get(piece_cid_v1, [])
            if len(allocs) == 0:
                reasons.append("missing active allocation for piece_cid_v1")
            elif len(allocs) > 1:
                reasons.append(f"duplicate active allocations for piece_cid_v1: {len(allocs)}")
            else:
                a = allocs[0]
                c.allocation_id = a.allocation_id
                c.alloc_term_min = a.term_min
                c.alloc_term_max = a.term_max
                c.alloc_expiration = a.expiration
                if a.client != client_id:
                    reasons.append(f"allocation client mismatch: {a.client} != {client_id}")
                if a.miner != provider_id:
                    reasons.append(f"allocation miner mismatch: {a.miner} != {provider_id}")
                if a.piece_size != piece_size_expected:
                    reasons.append(f"allocation piece_size mismatch: {a.piece_size} != {piece_size_expected}")
                if a.piece_cid != piece_cid_v1:
                    reasons.append("allocation piece CID mismatch")
                if a.term_min > duration:
                    reasons.append(f"allocation term_min > deal duration: {a.term_min} > {duration}")
                if a.term_max > duration:
                    reasons.append(f"allocation term_max > deal duration: {a.term_max} > {duration}")

            c.file_reject_reason = "; ".join(reasons) if reasons else None
            out.append(c)

    # Reject duplicate rows in the CSV itself. Mark all members, not just later occurrences.
    for label, attr in [("duplicate csv piece_cid_v1", "piece_cid_v1"), ("duplicate csv pcidv2", "piece_cid_v2")]:
        counts = Counter(getattr(c, attr) for c in out if getattr(c, attr))
        dup_values = {v for v, n in counts.items() if n > 1}
        for c in out:
            value = getattr(c, attr)
            if value in dup_values:
                extra = f"{label}: {value}"
                c.file_reject_reason = f"{c.file_reject_reason}; {extra}" if c.file_reject_reason else extra
    return out


def write_candidates_csv(path: Path, candidates: List[Candidate], provider: str, provider_id: int, deal_client: str, client_id: int, duration: int) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "csv_row_no", "deal_id", "deal_client", "allocation_client_id", "provider", "provider_id", "duration",
            "data_cid", "piece_cid_v1", "piece_cid_v2", "piece_size", "car_size", "car_url",
            "allocation_id", "alloc_term_min", "alloc_term_max", "alloc_expiration", "file_reject_reason",
            "data_json", "ddo_v1_json", "retrieval_v1_json", "pdp_v1_json",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for c in candidates:
            w.writerow(
                {
                    "csv_row_no": c.csv_row_no,
                    "deal_id": c.deal_id,
                    "deal_client": deal_client,
                    "allocation_client_id": client_id,
                    "provider": provider,
                    "provider_id": provider_id,
                    "duration": duration,
                    "data_cid": c.data_cid,
                    "piece_cid_v1": c.piece_cid_v1,
                    "piece_cid_v2": c.piece_cid_v2,
                    "piece_size": c.piece_size,
                    "car_size": c.car_size,
                    "car_url": c.car_url,
                    "allocation_id": c.allocation_id if c.allocation_id is not None else "\\N",
                    "alloc_term_min": c.alloc_term_min if c.alloc_term_min is not None else "\\N",
                    "alloc_term_max": c.alloc_term_max if c.alloc_term_max is not None else "\\N",
                    "alloc_expiration": c.alloc_expiration if c.alloc_expiration is not None else "\\N",
                    "file_reject_reason": c.file_reject_reason or "\\N",
                    "data_json": c.data_json(),
                    "ddo_v1_json": c.ddo_v1_json(provider, duration),
                    "retrieval_v1_json": c.retrieval_v1_json(),
                    "pdp_v1_json": "null",
                }
            )


def conflict_predicate(alias: str = "s") -> str:
    # Used in multiple places. This excludes every current DB artifact that would mean the deal/piece/allocation is already known.
    return f"""
  EXISTS (SELECT 1 FROM market_mk20_deal d WHERE d.id = {alias}.deal_id)
  OR EXISTS (SELECT 1 FROM market_mk20_pipeline_waiting w WHERE w.id = {alias}.deal_id)
  OR EXISTS (
    SELECT 1 FROM market_mk20_deal d
    WHERE d.piece_cid_v2 = {alias}.piece_cid_v2
       OR d.data #>> '{{piece_cid,/}}' = {alias}.piece_cid_v2
       OR d.ddo_v1 #>> '{{ddo,allocation_id}}' = {alias}.allocation_id::TEXT
  )
  OR EXISTS (
    SELECT 1
    FROM market_mk20_pipeline_waiting w
    JOIN market_mk20_deal d ON d.id = w.id
    WHERE d.piece_cid_v2 = {alias}.piece_cid_v2
       OR d.data #>> '{{piece_cid,/}}' = {alias}.piece_cid_v2
       OR d.ddo_v1 #>> '{{ddo,allocation_id}}' = {alias}.allocation_id::TEXT
  )
  OR EXISTS (
    SELECT 1 FROM market_mk20_download_pipeline p
    WHERE p.id = {alias}.deal_id OR p.piece_cid_v2 = {alias}.piece_cid_v2
  )
  OR EXISTS (
    SELECT 1 FROM market_mk20_pipeline p
    WHERE p.id = {alias}.deal_id
       OR p.piece_cid_v2 = {alias}.piece_cid_v2
       OR p.piece_cid = {alias}.piece_cid_v1
       OR p.allocation_id = {alias}.allocation_id
  )
  OR EXISTS (
    SELECT 1 FROM sectors_sdr_initial_pieces p
    WHERE p.sp_id = {alias}.provider_id
      AND (
        p.piece_cid = {alias}.piece_cid_v1
        OR p.piece_cid = {alias}.piece_cid_v2
        OR p.direct_piece_activation_manifest #>> '{{CID,/}}' = {alias}.piece_cid_v1
        OR p.direct_piece_activation_manifest #>> '{{CID,/}}' = {alias}.piece_cid_v2
        OR p.direct_piece_activation_manifest #>> '{{VerifiedAllocationKey,ID}}' = {alias}.allocation_id::TEXT
      )
  )
"""


def generate_stage_sql(validated_csv: Path, batch_name: str, stage_table: str, replace_stage: bool, reset_stage_table: bool) -> str:
    stage_table = sql_ident(stage_table)
    csv_path = str(validated_csv.resolve())
    reset_sql = f"DROP TABLE IF EXISTS {stage_table};" if reset_stage_table else ""
    delete_sql = f"DELETE FROM {stage_table} WHERE batch_name = {sql_literal(batch_name)};" if replace_stage else ""
    return f"""
BEGIN;

{reset_sql}

CREATE TABLE IF NOT EXISTS {stage_table} (
  batch_name TEXT NOT NULL,
  csv_row_no BIGINT NOT NULL,
  deal_id TEXT NOT NULL,
  deal_client TEXT NOT NULL,
  allocation_client_id BIGINT NOT NULL,
  provider TEXT NOT NULL,
  provider_id BIGINT NOT NULL,
  duration BIGINT NOT NULL,
  data_cid TEXT NOT NULL,
  piece_cid_v1 TEXT NOT NULL,
  piece_cid_v2 TEXT NOT NULL,
  piece_size BIGINT NOT NULL,
  car_size BIGINT NOT NULL,
  car_url TEXT NOT NULL,
  allocation_id BIGINT,
  alloc_term_min BIGINT,
  alloc_term_max BIGINT,
  alloc_expiration BIGINT,
  file_reject_reason TEXT,
  db_reject_reason TEXT,
  valid BOOLEAN NOT NULL DEFAULT FALSE,
  data_json JSONB NOT NULL,
  ddo_v1_json JSONB NOT NULL,
  retrieval_v1_json JSONB NOT NULL,
  pdp_v1_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (batch_name, csv_row_no)
);

{delete_sql}

CREATE TEMP TABLE mk20_import_tmp (
  csv_row_no BIGINT,
  deal_id TEXT,
  deal_client TEXT,
  allocation_client_id BIGINT,
  provider TEXT,
  provider_id BIGINT,
  duration BIGINT,
  data_cid TEXT,
  piece_cid_v1 TEXT,
  piece_cid_v2 TEXT,
  piece_size BIGINT,
  car_size BIGINT,
  car_url TEXT,
  allocation_id BIGINT,
  alloc_term_min BIGINT,
  alloc_term_max BIGINT,
  alloc_expiration BIGINT,
  file_reject_reason TEXT,
  data_json JSONB,
  ddo_v1_json JSONB,
  retrieval_v1_json JSONB,
  pdp_v1_json JSONB
) ON COMMIT DROP;

\\copy mk20_import_tmp (csv_row_no, deal_id, deal_client, allocation_client_id, provider, provider_id, duration, data_cid, piece_cid_v1, piece_cid_v2, piece_size, car_size, car_url, allocation_id, alloc_term_min, alloc_term_max, alloc_expiration, file_reject_reason, data_json, ddo_v1_json, retrieval_v1_json, pdp_v1_json) FROM {sql_literal(csv_path)} WITH (FORMAT csv, HEADER true, NULL '\\N')

INSERT INTO {stage_table} (
  batch_name, csv_row_no, deal_id, deal_client, allocation_client_id, provider, provider_id, duration,
  data_cid, piece_cid_v1, piece_cid_v2, piece_size, car_size, car_url,
  allocation_id, alloc_term_min, alloc_term_max, alloc_expiration,
  file_reject_reason, data_json, ddo_v1_json, retrieval_v1_json, pdp_v1_json
)
SELECT
  {sql_literal(batch_name)}, csv_row_no, deal_id, deal_client, allocation_client_id, provider, provider_id, duration,
  data_cid, piece_cid_v1, piece_cid_v2, piece_size, car_size, car_url,
  allocation_id, alloc_term_min, alloc_term_max, alloc_expiration,
  file_reject_reason, data_json, ddo_v1_json, retrieval_v1_json, pdp_v1_json
FROM mk20_import_tmp;

-- DB-side rejection checks. First mark specific reasons for easier auditing.
UPDATE {stage_table} s
SET db_reject_reason = 'deal id already exists in market_mk20_deal'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (SELECT 1 FROM market_mk20_deal d WHERE d.id = s.deal_id);

UPDATE {stage_table} s
SET db_reject_reason = 'deal id already exists in market_mk20_pipeline_waiting'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (SELECT 1 FROM market_mk20_pipeline_waiting w WHERE w.id = s.deal_id);

UPDATE {stage_table} s
SET db_reject_reason = 'piece or allocation already exists in market_mk20_deal'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1 FROM market_mk20_deal d
    WHERE d.piece_cid_v2 = s.piece_cid_v2
       OR d.data #>> '{{piece_cid,/}}' = s.piece_cid_v2
       OR d.ddo_v1 #>> '{{ddo,allocation_id}}' = s.allocation_id::TEXT
  );

UPDATE {stage_table} s
SET db_reject_reason = 'piece or allocation already in market_mk20_pipeline_waiting'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1
    FROM market_mk20_pipeline_waiting w
    JOIN market_mk20_deal d ON d.id = w.id
    WHERE d.piece_cid_v2 = s.piece_cid_v2
       OR d.data #>> '{{piece_cid,/}}' = s.piece_cid_v2
       OR d.ddo_v1 #>> '{{ddo,allocation_id}}' = s.allocation_id::TEXT
  );

UPDATE {stage_table} s
SET db_reject_reason = 'piece or allocation already exists in market_mk20_download_pipeline'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1 FROM market_mk20_download_pipeline p
    WHERE p.id = s.deal_id OR p.piece_cid_v2 = s.piece_cid_v2
  );

UPDATE {stage_table} s
SET db_reject_reason = 'piece or allocation already exists in market_mk20_pipeline'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1 FROM market_mk20_pipeline p
    WHERE p.id = s.deal_id
       OR p.piece_cid_v2 = s.piece_cid_v2
       OR p.piece_cid = s.piece_cid_v1
       OR p.allocation_id = s.allocation_id
  );

UPDATE {stage_table} s
SET db_reject_reason = 'piece or allocation already exists in sectors_sdr_initial_pieces'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1 FROM sectors_sdr_initial_pieces p
    WHERE p.sp_id = s.provider_id
      AND (
        p.piece_cid = s.piece_cid_v1
        OR p.piece_cid = s.piece_cid_v2
        OR p.direct_piece_activation_manifest #>> '{{CID,/}}' = s.piece_cid_v1
        OR p.direct_piece_activation_manifest #>> '{{CID,/}}' = s.piece_cid_v2
        OR p.direct_piece_activation_manifest #>> '{{VerifiedAllocationKey,ID}}' = s.allocation_id::TEXT
      )
  );

UPDATE {stage_table}
SET valid = (file_reject_reason IS NULL AND db_reject_reason IS NULL)
WHERE batch_name = {sql_literal(batch_name)};

COMMIT;

SELECT 'stage_summary' AS section,
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE valid) AS valid,
       COUNT(*) FILTER (WHERE NOT valid) AS rejected
FROM {stage_table}
WHERE batch_name = {sql_literal(batch_name)};

SELECT 'reject_summary' AS section,
       COALESCE(file_reject_reason, db_reject_reason, 'valid') AS reason,
       COUNT(*) AS count
FROM {stage_table}
WHERE batch_name = {sql_literal(batch_name)}
GROUP BY 1, 2
ORDER BY count DESC, reason;

SELECT 'valid_sample' AS section, csv_row_no, deal_id, deal_client, allocation_client_id, provider, provider_id, piece_cid_v1, piece_cid_v2, allocation_id, car_url
FROM {stage_table}
WHERE batch_name = {sql_literal(batch_name)} AND valid
ORDER BY csv_row_no
LIMIT 20;
"""


def generate_insert_sql(batch_name: str, stage_table: str, limit: Optional[int], run_id: str) -> str:
    stage_table = sql_ident(stage_table)
    limit_clause = f"LIMIT {int(limit)}" if limit is not None and limit > 0 else ""
    return f"""
BEGIN;

CREATE TABLE IF NOT EXISTS audit_mk20_import_runs (
  run_id TEXT PRIMARY KEY,
  batch_name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  limit_rows BIGINT,
  expected_rows BIGINT,
  inserted_deals BIGINT,
  inserted_waiting BIGINT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS audit_mk20_import_inserted (
  run_id TEXT NOT NULL,
  batch_name TEXT NOT NULL,
  csv_row_no BIGINT NOT NULL,
  deal_id TEXT NOT NULL,
  deal_client TEXT NOT NULL,
  allocation_client_id BIGINT NOT NULL,
  provider TEXT NOT NULL,
  provider_id BIGINT NOT NULL,
  piece_cid_v1 TEXT NOT NULL,
  piece_cid_v2 TEXT NOT NULL,
  allocation_id BIGINT NOT NULL,
  car_url TEXT NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, deal_id)
);

-- Refuse run_id reuse. If this fails, choose a new --run-id.
INSERT INTO audit_mk20_import_runs (run_id, batch_name, limit_rows, notes)
VALUES ({sql_literal(run_id)}, {sql_literal(batch_name)}, {sql_literal(limit) if limit else 'NULL'}, {sql_literal('mk20 db importer ' + VERSION)});

CREATE TEMP TABLE picked AS
SELECT *
FROM {stage_table} s
WHERE s.batch_name = {sql_literal(batch_name)}
  AND s.valid = TRUE
  AND NOT ({conflict_predicate('s')})
ORDER BY s.csv_row_no
{limit_clause};

-- Abort if no rows were picked. This prevents accidental empty success.
DO $$
DECLARE n BIGINT;
BEGIN
  SELECT COUNT(*) INTO n FROM picked;
  IF n = 0 THEN
    RAISE EXCEPTION 'No valid picked rows for run_id={run_id} batch={batch_name}';
  END IF;
END $$;

WITH ins_deal AS (
  -- market_mk20_deal.created_at defaults to timezone('UTC', now()) in some Curio/YB schemas.
  -- In a non-UTC session that stores a timestamptz 9h behind KST.
  -- Explicit now() preserves the actual transaction timestamp.
  INSERT INTO market_mk20_deal (
    created_at, id, client, piece_cid_v2, data, ddo_v1, retrieval_v1, pdp_v1
  )
  SELECT
    now(),
    deal_id,
    deal_client,
    piece_cid_v2,
    data_json,
    ddo_v1_json,
    retrieval_v1_json,
    pdp_v1_json
  FROM picked
  RETURNING id
), ins_waiting AS (
  INSERT INTO market_mk20_pipeline_waiting (id)
  SELECT id FROM ins_deal
  ON CONFLICT (id) DO NOTHING
  RETURNING id
), ins_audit AS (
  INSERT INTO audit_mk20_import_inserted (
    run_id, batch_name, csv_row_no, deal_id, deal_client, allocation_client_id, provider, provider_id,
    piece_cid_v1, piece_cid_v2, allocation_id, car_url
  )
  SELECT
    {sql_literal(run_id)}, {sql_literal(batch_name)}, p.csv_row_no, p.deal_id, p.deal_client, p.allocation_client_id,
    p.provider, p.provider_id, p.piece_cid_v1, p.piece_cid_v2, p.allocation_id, p.car_url
  FROM picked p
  JOIN ins_deal d ON d.id = p.deal_id
  RETURNING deal_id
)
UPDATE audit_mk20_import_runs r
SET expected_rows = (SELECT COUNT(*) FROM picked),
    inserted_deals = (SELECT COUNT(*) FROM ins_deal),
    inserted_waiting = (SELECT COUNT(*) FROM ins_waiting)
WHERE r.run_id = {sql_literal(run_id)};

-- Abort if anything other than exact picked/deal/waiting equality happened.
DO $$
DECLARE p BIGINT; d BIGINT; w BIGINT;
BEGIN
  SELECT expected_rows, inserted_deals, inserted_waiting INTO p, d, w
  FROM audit_mk20_import_runs WHERE run_id = {sql_literal(run_id)};
  IF p IS NULL OR d IS NULL OR w IS NULL OR p <> d OR p <> w THEN
    RAISE EXCEPTION 'Insert count mismatch for run_id={run_id}: picked %, deal %, waiting %', p, d, w;
  END IF;
END $$;

COMMIT;

SELECT 'insert_result' AS section, run_id, batch_name, expected_rows, inserted_deals, inserted_waiting
FROM audit_mk20_import_runs
WHERE run_id = {sql_literal(run_id)};
"""


def generate_verify_sql(batch_name: str, stage_table: str, run_id: str) -> str:
    stage_table = sql_ident(stage_table)
    return f"""
-- Verification for mk20 importer run_id={run_id}
\nSELECT 'run_manifest' AS section, *
FROM audit_mk20_import_runs
WHERE run_id = {sql_literal(run_id)};

SELECT 'inserted_audit_count' AS section, COUNT(*) AS count
FROM audit_mk20_import_inserted
WHERE run_id = {sql_literal(run_id)};

SELECT 'deal_rows' AS section, COUNT(*) AS count
FROM market_mk20_deal d
JOIN audit_mk20_import_inserted i ON i.deal_id = d.id
WHERE i.run_id = {sql_literal(run_id)};

SELECT 'waiting_rows' AS section, COUNT(*) AS count
FROM market_mk20_pipeline_waiting w
JOIN audit_mk20_import_inserted i ON i.deal_id = w.id
WHERE i.run_id = {sql_literal(run_id)};

SELECT 'download_pipeline_rows' AS section, COUNT(*) AS count
FROM market_mk20_download_pipeline p
JOIN audit_mk20_import_inserted i ON i.deal_id = p.id
WHERE i.run_id = {sql_literal(run_id)};

SELECT 'mk20_pipeline_rows' AS section, COUNT(*) AS count
FROM market_mk20_pipeline p
JOIN audit_mk20_import_inserted i ON i.deal_id = p.id
WHERE i.run_id = {sql_literal(run_id)};

-- These must return zero rows.
SELECT 'bad_client' AS problem, d.id, d.client, i.deal_client
FROM market_mk20_deal d
JOIN audit_mk20_import_inserted i ON i.deal_id = d.id
WHERE i.run_id = {sql_literal(run_id)} AND d.client <> i.deal_client;

SELECT 'bad_provider_or_allocation' AS problem, d.id,
       d.ddo_v1 #>> '{{ddo,provider}}' AS provider,
       i.provider AS expected_provider,
       d.ddo_v1 #>> '{{ddo,allocation_id}}' AS allocation_id,
       i.allocation_id AS expected_allocation_id
FROM market_mk20_deal d
JOIN audit_mk20_import_inserted i ON i.deal_id = d.id
WHERE i.run_id = {sql_literal(run_id)}
  AND (
    d.ddo_v1 #>> '{{ddo,provider}}' <> i.provider
    OR d.ddo_v1 #>> '{{ddo,allocation_id}}' <> i.allocation_id::TEXT
  );

SELECT 'bad_piece_or_url' AS problem, d.id,
       d.piece_cid_v2, i.piece_cid_v2 AS expected_piece_cid_v2,
       d.data #>> '{{source_http,urls,0,url}}' AS url,
       i.car_url AS expected_url
FROM market_mk20_deal d
JOIN audit_mk20_import_inserted i ON i.deal_id = d.id
WHERE i.run_id = {sql_literal(run_id)}
  AND (
    d.piece_cid_v2 <> i.piece_cid_v2
    OR d.data #>> '{{piece_cid,/}}' <> i.piece_cid_v2
    OR d.data #>> '{{source_http,urls,0,url}}' <> i.car_url
  );

SELECT 'duplicate_piece_cid_v2_in_market_mk20_deal' AS problem, d.piece_cid_v2, COUNT(*) AS count
FROM market_mk20_deal d
WHERE d.piece_cid_v2 IN (SELECT piece_cid_v2 FROM audit_mk20_import_inserted WHERE run_id = {sql_literal(run_id)})
GROUP BY d.piece_cid_v2
HAVING COUNT(*) > 1;

SELECT 'duplicate_allocation_in_market_mk20_deal' AS problem, d.ddo_v1 #>> '{{ddo,allocation_id}}' AS allocation_id, COUNT(*) AS count
FROM market_mk20_deal d
WHERE d.ddo_v1 #>> '{{ddo,allocation_id}}' IN (
  SELECT allocation_id::TEXT FROM audit_mk20_import_inserted WHERE run_id = {sql_literal(run_id)}
)
GROUP BY d.ddo_v1 #>> '{{ddo,allocation_id}}'
HAVING COUNT(*) > 1;

SELECT 'sample_inserted_deals' AS section,
       i.csv_row_no, d.id, d.client, d.piece_cid_v2,
       d.ddo_v1 #>> '{{ddo,provider}}' AS provider,
       d.ddo_v1 #>> '{{ddo,allocation_id}}' AS allocation_id,
       w.id IS NOT NULL AS still_waiting,
       dp.id IS NOT NULL AS in_download_pipeline,
       mp.id IS NOT NULL AS in_mk20_pipeline
FROM audit_mk20_import_inserted i
JOIN market_mk20_deal d ON d.id = i.deal_id
LEFT JOIN market_mk20_pipeline_waiting w ON w.id = i.deal_id
LEFT JOIN market_mk20_download_pipeline dp ON dp.id = i.deal_id
LEFT JOIN market_mk20_pipeline mp ON mp.id = i.deal_id
WHERE i.run_id = {sql_literal(run_id)}
ORDER BY i.csv_row_no
LIMIT 50;
"""


def generate_rollback_sql(batch_name: str, run_id: str) -> str:
    return f"""
-- Rollback for mk20 importer run_id={run_id}
-- Safe intent: remove only rows created for this run_id, identified by audit_mk20_import_inserted.
-- It refuses to run if any imported piece has already reached sectors_sdr_initial_pieces.

BEGIN;

DO $$
DECLARE blockers BIGINT;
BEGIN
  SELECT COUNT(*) INTO blockers
  FROM sectors_sdr_initial_pieces p
  JOIN audit_mk20_import_inserted i
    ON p.sp_id = i.provider_id
   AND (
        p.piece_cid = i.piece_cid_v1
        OR p.piece_cid = i.piece_cid_v2
        OR p.direct_piece_activation_manifest #>> '{{VerifiedAllocationKey,ID}}' = i.allocation_id::TEXT
   )
  WHERE i.run_id = {sql_literal(run_id)};

  IF blockers > 0 THEN
    RAISE EXCEPTION 'Refusing rollback: % imported rows already reached sectors_sdr_initial_pieces. Manual recovery required.', blockers;
  END IF;
END $$;

CREATE TEMP TABLE rollback_ids AS
SELECT deal_id, piece_cid_v1, piece_cid_v2, allocation_id, car_url
FROM audit_mk20_import_inserted
WHERE run_id = {sql_literal(run_id)};

CREATE TEMP TABLE rollback_ref_ids AS
SELECT DISTINCT unnest(p.ref_ids) AS ref_id
FROM market_mk20_download_pipeline p
JOIN rollback_ids r ON r.deal_id = p.id;

DELETE FROM market_mk20_pipeline_waiting w USING rollback_ids r WHERE w.id = r.deal_id;
DELETE FROM market_mk20_pipeline p USING rollback_ids r WHERE p.id = r.deal_id;
DELETE FROM market_mk20_download_pipeline p USING rollback_ids r WHERE p.id = r.deal_id;
DELETE FROM parked_piece_refs pr USING rollback_ref_ids rr WHERE pr.ref_id = rr.ref_id;
DELETE FROM market_mk20_deal d USING rollback_ids r WHERE d.id = r.deal_id;

-- Keep audit rows intentionally, so the rollback remains traceable.
UPDATE audit_mk20_import_runs
SET notes = COALESCE(notes, '') || ' | rollback executed at ' || now()::TEXT
WHERE run_id = {sql_literal(run_id)};

COMMIT;

SELECT 'rollback_remaining_deals' AS section, COUNT(*) AS count
FROM market_mk20_deal d
JOIN audit_mk20_import_inserted i ON i.deal_id = d.id
WHERE i.run_id = {sql_literal(run_id)};

SELECT 'rollback_remaining_waiting' AS section, COUNT(*) AS count
FROM market_mk20_pipeline_waiting w
JOIN audit_mk20_import_inserted i ON i.deal_id = w.id
WHERE i.run_id = {sql_literal(run_id)};

SELECT 'rollback_remaining_download_pipeline' AS section, COUNT(*) AS count
FROM market_mk20_download_pipeline p
JOIN audit_mk20_import_inserted i ON i.deal_id = p.id
WHERE i.run_id = {sql_literal(run_id)};

SELECT 'rollback_remaining_mk20_pipeline' AS section, COUNT(*) AS count
FROM market_mk20_pipeline p
JOIN audit_mk20_import_inserted i ON i.deal_id = p.id
WHERE i.run_id = {sql_literal(run_id)};
"""


def generate_observe_sql(batch_name: str, run_id: str) -> str:
    return f"""
SELECT 'waiting_joined' AS section, COUNT(*) AS count
FROM market_mk20_pipeline_waiting w
JOIN audit_mk20_import_inserted i ON i.deal_id = w.id
WHERE i.run_id = {sql_literal(run_id)};

SELECT 'download_pipeline' AS section, COUNT(*) AS count
FROM market_mk20_download_pipeline p
JOIN audit_mk20_import_inserted i ON i.deal_id = p.id
WHERE i.run_id = {sql_literal(run_id)};

SELECT 'mk20_pipeline' AS section, COUNT(*) AS count
FROM market_mk20_pipeline p
JOIN audit_mk20_import_inserted i ON i.deal_id = p.id
WHERE i.run_id = {sql_literal(run_id)};

SELECT 'sdr_initial_pieces' AS section, COUNT(*) AS count
FROM sectors_sdr_initial_pieces p
JOIN audit_mk20_import_inserted i
  ON p.sp_id = i.provider_id
 AND (
      p.piece_cid = i.piece_cid_v1
      OR p.piece_cid = i.piece_cid_v2
      OR p.direct_piece_activation_manifest #>> '{{VerifiedAllocationKey,ID}}' = i.allocation_id::TEXT
 )
WHERE i.run_id = {sql_literal(run_id)};

SELECT i.csv_row_no, i.deal_id, i.piece_cid_v1, i.piece_cid_v2, i.allocation_id,
       w.id IS NOT NULL AS still_waiting,
       dp.id IS NOT NULL AS in_download_pipeline,
       mp.id IS NOT NULL AS in_mk20_pipeline
FROM audit_mk20_import_inserted i
LEFT JOIN market_mk20_pipeline_waiting w ON w.id = i.deal_id
LEFT JOIN market_mk20_download_pipeline dp ON dp.id = i.deal_id
LEFT JOIN market_mk20_pipeline mp ON mp.id = i.deal_id
WHERE i.run_id = {sql_literal(run_id)}
ORDER BY i.csv_row_no
LIMIT 50;
"""


def run_psql(psql_cmd: str, sql_file: Path) -> None:
    cmd = f"{psql_cmd} -v ON_ERROR_STOP=1 -f {shlex.quote(str(sql_file))}"
    eprint(f"+ {cmd}")
    subprocess.run(cmd, shell=True, check=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="DB-safe Curio MK20 DDO staging/import tool")
    ap.add_argument("--allocations", required=True, type=Path)
    ap.add_argument("--csv", required=True, type=Path)
    ap.add_argument("--batch-name", default="mk20-batch")
    ap.add_argument("--run-id", default=None, help="audit run id; default auto-generated")
    ap.add_argument("--out-dir", default="mk20-import-out", type=Path)
    ap.add_argument("--deal-client", required=True, help="MK20 Deal.Client / market_mk20_deal.client, usually f1 wallet")
    ap.add_argument("--client-id", required=True, type=int, help="numeric DataCap allocation client actor ID used only for allocation validation")
    ap.add_argument("--provider", required=True, help="provider/miner address, for example f0...")
    ap.add_argument("--provider-id", required=True, type=int, help="numeric provider/miner actor ID")
    ap.add_argument("--piece-size", default=34359738368, type=int)
    ap.add_argument("--duration", default=5256000, type=int)
    ap.add_argument("--stage-table", default="audit_mk20_import_stage")
    ap.add_argument("--replace-stage", action="store_true", help="delete prior rows for this batch_name from the stage table before loading")
    ap.add_argument("--reset-stage-table", action="store_true", help="drop and recreate the stage table before loading; useful after schema upgrades")
    ap.add_argument("--id-time-ms", type=int, default=None, help="ULID timestamp in ms; default current time")
    ap.add_argument("--no-db", action="store_true", help="only validate files and generate SQL")
    ap.add_argument("--psql-cmd", default="ysql_curio")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--ack-db-direct", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--allow-full-batch", action="store_true")
    args = ap.parse_args()

    sql_ident(args.stage_table)
    if args.execute:
        if not args.ack_db_direct:
            raise SystemExit("Refusing production insert: --execute requires --ack-db-direct")
        if (args.limit is None or args.limit <= 0) and not args.allow_full_batch:
            raise SystemExit("Refusing full batch insert: specify --limit, or add --allow-full-batch intentionally")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    id_time_ms = args.id_time_ms if args.id_time_ms is not None else int(time.time() * 1000)
    run_id = args.run_id or time.strftime(f"{args.batch_name}_%Y%m%d_%H%M%S")

    allocations = read_allocations(args.allocations)
    allocations_by_piece: Dict[str, List[Allocation]] = defaultdict(list)
    for a in allocations:
        if a.piece_cid:
            allocations_by_piece[a.piece_cid].append(a)

    candidates = read_csv_candidates(
        args.csv, args.batch_name, id_time_ms, allocations_by_piece,
        args.client_id, args.provider_id, args.piece_size, args.duration,
    )

    total = len(candidates)
    file_valid = sum(1 for c in candidates if c.file_reject_reason is None)
    file_rejected = total - file_valid
    reject_counts = Counter(c.file_reject_reason or "valid_file" for c in candidates)

    prefix = args.out_dir / f"{args.batch_name}.{run_id}"
    validated_csv = Path(str(prefix) + ".validated.csv")
    stage_sql = Path(str(prefix) + ".stage.sql")
    insert_sql = Path(str(prefix) + ".insert.sql")
    verify_sql = Path(str(prefix) + ".verify.sql")
    rollback_sql = Path(str(prefix) + ".rollback.sql")
    observe_sql = Path(str(prefix) + ".observe.sql")

    write_candidates_csv(validated_csv, candidates, args.provider, args.provider_id, args.deal_client, args.client_id, args.duration)
    stage_sql.write_text(generate_stage_sql(validated_csv, args.batch_name, args.stage_table, args.replace_stage, args.reset_stage_table), encoding="utf-8")
    insert_sql.write_text(generate_insert_sql(args.batch_name, args.stage_table, args.limit, run_id), encoding="utf-8")
    verify_sql.write_text(generate_verify_sql(args.batch_name, args.stage_table, run_id), encoding="utf-8")
    rollback_sql.write_text(generate_rollback_sql(args.batch_name, run_id), encoding="utf-8")
    observe_sql.write_text(generate_observe_sql(args.batch_name, run_id), encoding="utf-8")

    print(f"mk20_db_importer version: {VERSION}")
    print(f"run id: {run_id}")
    print(f"allocations: {len(allocations)} records")
    print(f"csv rows: {total}")
    print(f"file-valid candidates: {file_valid}")
    print(f"file-rejected candidates: {file_rejected}")
    print(f"deal client: {args.deal_client}")
    print(f"allocation client id: {args.client_id}")
    print("top file validation results:")
    for reason, n in reject_counts.most_common(12):
        print(f"  {n:6d}  {reason}")
    for p in [validated_csv, stage_sql, insert_sql, verify_sql, rollback_sql, observe_sql]:
        print(f"wrote: {p}")

    if args.no_db:
        print("no-db mode: not running psql")
        return 0

    run_psql(args.psql_cmd, stage_sql)
    if args.execute:
        run_psql(args.psql_cmd, insert_sql)
        print(f"production insert complete for run id: {run_id}")
        print(f"verify with:   {args.psql_cmd} -v ON_ERROR_STOP=1 -f {verify_sql}")
        print(f"rollback with: {args.psql_cmd} -v ON_ERROR_STOP=1 -f {rollback_sql}")
    else:
        print("dry-run DB staging complete: production insert not executed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
