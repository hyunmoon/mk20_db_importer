#!/usr/bin/env python3
"""
mk20_db_importer.py

DB-safe Curio MK20 DDO importer for SP/operator-side enqueue.

Safety model:
  - Default is no production insert.
  - Production insert requires --execute and --ack-db-direct.
  - Full batch additionally requires --allow-full-batch.
  - Stage excludes anything already present in current Curio DB by deal id or allocation id.
  - Piece CID conflicts are scoped to the same provider/miner so cross-provider replicas are allowed.
  - Stage also checks waiting queue, download pipeline, mk20 pipeline, and sectors_sdr_initial_pieces.
  - Insert SQL rechecks conflicts at insert time.
  - Each production insert has a run_id and writes an audit manifest.
  - Rollback and verify SQL are generated for that run_id.

Important: deal_client is the MK20 Deal.Client / market_mk20_deal.client value, normally the f1 wallet
that sptool used as --wallet. client_id is the numeric DataCap allocation client actor id.
"""
from __future__ import annotations

import argparse
import base64
import binascii
import csv
import hashlib
import json
import posixpath
import re
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
VERSION = "2026-09-06-allocation-derived-start-epoch"
EXPECTED_CSV_COLUMNS = ["data_cid", "piece_cid_v1", "pcidv2", "piece_size", "car_size", "car_url"]
STAGE_TABLE_PREFIX = "audit_mk20_import_"
DEFAULT_START_BEFORE_ALLOCATION_EXPIRATION_EPOCHS = 1440
DEFAULT_EXPECTED_SEAL_RUNWAY_EPOCHS = 960
CID_VERSION = 1
RAW_CODEC = 0x55
FIL_COMMITMENT_UNSEALED_CODEC = 0xF101
PIECE_CID_V2_MULTIHASH = 0x1011
PIECE_CID_V1_MULTIHASH = 0x1012


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def sql_ident(name: str) -> str:
    # PostgreSQL truncates identifiers after 63 bytes. Restricting identifiers to
    # ASCII and the server limit prevents lookalikes and truncation collisions.
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


def safe_label(value: str, label: str) -> str:
    # Labels become filename components and audit keys. Preserve human-readable
    # labels (including spaces) while rejecting path separators and line/control
    # characters that can escape the output directory or generated SQL comments.
    if (
        not value
        or len(value) > 128
        or any(forbidden in value for forbidden in ("\r", "\n", "\x00", "/", "\\"))
    ):
        raise ValueError(
            f"unsafe {label}: {value!r}; use 1-128 characters without CR, LF, NUL, '/', or '\\'"
        )
    return value


def validate_provider_id_address(provider: str, provider_id: int) -> int:
    match = re.fullmatch(r"[ft]0([0-9]+)", provider)
    if match is None:
        raise ValueError("--provider must be a Filecoin ID address (f0... or t0...)")
    address_id = int(match.group(1))
    if address_id != provider_id:
        raise ValueError(
            f"--provider {provider!r} resolves to actor ID {address_id}, "
            f"not --provider-id {provider_id}"
        )
    return address_id


def validate_stage_table(name: str) -> str:
    name = sql_ident(name)
    if not name.startswith(STAGE_TABLE_PREFIX):
        raise ValueError(
            f"unsafe stage table {name!r}: it must start with {STAGE_TABLE_PREFIX!r}"
        )
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


@dataclass(frozen=True)
class PieceCidV2Info:
    piece_cid_v1: str
    padded_size: int
    payload_size: int
    padding: int
    tree_height: int


def _read_uvarint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    for index in range(10):
        position = offset + index
        if position >= len(data):
            raise ValueError("truncated unsigned varint")
        byte = data[position]
        if index == 9 and byte > 1:
            raise ValueError("unsigned varint exceeds 64 bits")
        value |= (byte & 0x7F) << (7 * index)
        if byte < 0x80:
            if index > 0 and byte == 0:
                raise ValueError("non-canonical unsigned varint")
            return value, position + 1
    raise ValueError("unsigned varint exceeds 10 bytes")


def _encode_uvarint(value: int) -> bytes:
    if value < 0:
        raise ValueError("cannot encode a negative unsigned varint")
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _decode_base32_cid(value: str) -> bytes:
    if not value.startswith("b") or value != value.lower():
        raise ValueError("CID must use canonical lowercase base32 multibase")
    payload = value[1:]
    if not payload:
        raise ValueError("CID has no base32 payload")
    padded = payload.upper() + "=" * ((8 - len(payload) % 8) % 8)
    try:
        decoded = base64.b32decode(padded, casefold=False)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("CID has invalid base32 data") from exc
    if _encode_base32_cid(decoded) != value:
        raise ValueError("CID is not canonical base32")
    return decoded


def _encode_base32_cid(value: bytes) -> str:
    return "b" + base64.b32encode(value).decode("ascii").lower().rstrip("=")


def piece_cid_v2_info(piece_cid_v2: str) -> PieceCidV2Info:
    data = _decode_base32_cid(piece_cid_v2)
    version, offset = _read_uvarint(data, 0)
    codec, offset = _read_uvarint(data, offset)
    multihash_code, offset = _read_uvarint(data, offset)
    digest_length, offset = _read_uvarint(data, offset)
    if version != CID_VERSION:
        raise ValueError(f"PieceCIDv2 must be CIDv1, got version {version}")
    if codec != RAW_CODEC:
        raise ValueError(f"PieceCIDv2 must use raw codec 0x{RAW_CODEC:x}")
    if multihash_code != PIECE_CID_V2_MULTIHASH:
        raise ValueError(
            f"PieceCIDv2 must use multihash 0x{PIECE_CID_V2_MULTIHASH:x}"
        )
    digest = data[offset:]
    if len(digest) != digest_length:
        raise ValueError("PieceCIDv2 multihash digest length is inconsistent")

    padding, digest_offset = _read_uvarint(digest, 0)
    if digest_offset >= len(digest):
        raise ValueError("PieceCIDv2 digest is missing tree height")
    tree_height = digest[digest_offset]
    root = digest[digest_offset + 1 :]
    if len(root) != 32:
        raise ValueError("PieceCIDv2 commitment root must be 32 bytes")
    if tree_height < 2:
        raise ValueError("PieceCIDv2 tree height is below the minimum piece size")

    padded_size = 32 << tree_height
    if padded_size > (1 << 63) - 1:
        raise ValueError("PieceCIDv2 padded size exceeds signed BIGINT range")
    unpadded_capacity = padded_size * 127 // 128
    half_unpadded_capacity = unpadded_capacity >> 1
    # Match go-fil-commcid PieceCidV2ToDataCommitment exactly. Padding must
    # describe the smaller half of the selected tree; equality is invalid.
    if padding >= half_unpadded_capacity:
        raise ValueError(
            "PieceCIDv2 padding must be less than half the unpadded piece capacity"
        )
    payload_size = unpadded_capacity - padding

    v1_bytes = b"".join(
        [
            _encode_uvarint(CID_VERSION),
            _encode_uvarint(FIL_COMMITMENT_UNSEALED_CODEC),
            _encode_uvarint(PIECE_CID_V1_MULTIHASH),
            _encode_uvarint(len(root)),
            root,
        ]
    )
    return PieceCidV2Info(
        piece_cid_v1=_encode_base32_cid(v1_bytes),
        padded_size=padded_size,
        payload_size=payload_size,
        padding=padding,
        tree_height=tree_height,
    )


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
    start_epoch: Optional[int] = None
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
                    "start_epoch": self.start_epoch,
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


def derive_allocation_start_epoch(
    allocation_expiration: int,
    chain_head: int,
    start_before_allocation_expiration_epochs: int,
    expected_seal_runway_epochs: int,
) -> tuple[int, Optional[str]]:
    start_epoch = allocation_expiration - start_before_allocation_expiration_epochs
    if start_epoch <= 0:
        return start_epoch, (
            "allocation-derived start epoch is non-positive: "
            f"start_epoch={start_epoch}, allocation_expiration={allocation_expiration}, "
            "start_before_allocation_expiration_epochs="
            f"{start_before_allocation_expiration_epochs}"
        )
    if start_epoch < chain_head:
        return start_epoch, (
            "allocation-derived start epoch is in the past: "
            f"start_epoch={start_epoch}, chain_head={chain_head}"
        )

    minimum_start_epoch = chain_head + expected_seal_runway_epochs
    if start_epoch < minimum_start_epoch:
        return start_epoch, (
            "insufficient sealing runway: "
            f"start_epoch={start_epoch}, minimum_start_epoch={minimum_start_epoch}, "
            f"chain_head={chain_head}, "
            f"expected_seal_runway_epochs={expected_seal_runway_epochs}"
        )
    return start_epoch, None


def read_allocations(path: Path) -> List[Allocation]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if isinstance(obj, dict):
        # Support both FilPlus list forms:
        #   {"allocations": {"123": {...}}}
        #   {"Allocations": {"123": {...}}}
        # and older flat mappings keyed by allocation id.
        nested = get_any(obj, ["allocations", "Allocations"])
        if isinstance(nested, dict):
            records = []
            for aid, rec in nested.items():
                if not isinstance(rec, dict):
                    continue
                rec2 = dict(rec)
                rec2.setdefault("allocationid", aid)
                rec2.setdefault("allocation_id", aid)
                records.append(rec2)
        elif isinstance(nested, list):
            records = nested
        elif nested is not None:
            raise ValueError("'allocations' must be a list or object")
        else:
            records = []
            for aid, rec in obj.items():
                if not isinstance(rec, dict):
                    continue
                rec2 = dict(rec)
                rec2.setdefault("allocationid", aid)
                rec2.setdefault("allocation_id", aid)
                records.append(rec2)
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
            allocation_id = int(aid)
            if allocation_id <= 0:
                raise ValueError("allocation_id must be positive")
            out.append(
                Allocation(
                    allocation_id=allocation_id,
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


def car_url_validation_error(piece_cid_v1: str, car_url: str) -> Optional[str]:
    try:
        parsed = urlparse(car_url)
        hostname = parsed.hostname
    except ValueError:
        return "car_url is malformed"
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc or not hostname:
        return "car_url must be an absolute http(s) URL"
    if parsed.username is not None or parsed.password is not None:
        return "car_url must not contain embedded credentials"
    if piece_cid_v1 and not car_url_filename_matches(piece_cid_v1, car_url):
        return "car_url basename does not equal <piece_cid_v1>.car"
    return None


def read_csv_candidates(
    path: Path,
    batch_name: str,
    id_time_ms: int,
    allocations_by_piece: Dict[str, List[Allocation]],
    client_id: int,
    provider_id: int,
    piece_size_expected: int,
    duration: int,
    chain_head: int,
    start_before_allocation_expiration_epochs: int,
    expected_seal_runway_epochs: int,
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
                if car_size <= 0:
                    reasons.append("car_size must be positive")
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
            if piece_cid_v2:
                try:
                    piece_info = piece_cid_v2_info(piece_cid_v2)
                except ValueError as exc:
                    reasons.append(f"invalid pcidv2: {exc}")
                else:
                    if piece_cid_v1 and piece_info.piece_cid_v1 != piece_cid_v1:
                        reasons.append(
                            "pcidv2 commitment does not match piece_cid_v1"
                        )
                    if piece_size >= 0 and piece_info.padded_size != piece_size:
                        reasons.append(
                            "pcidv2 padded size does not match csv piece_size: "
                            f"{piece_info.padded_size} != {piece_size}"
                        )
            if car_url:
                url_error = car_url_validation_error(piece_cid_v1, car_url)
                if url_error:
                    reasons.append(url_error)

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
                c.start_epoch, scheduling_error = derive_allocation_start_epoch(
                    a.expiration,
                    chain_head,
                    start_before_allocation_expiration_epochs,
                    expected_seal_runway_epochs,
                )
                if scheduling_error:
                    reasons.append(scheduling_error)
                if a.client != client_id:
                    reasons.append(f"allocation client mismatch: {a.client} != {client_id}")
                if a.miner != provider_id:
                    reasons.append(f"allocation miner mismatch: {a.miner} != {provider_id}")
                if a.piece_size != piece_size_expected:
                    reasons.append(f"allocation piece_size mismatch: {a.piece_size} != {piece_size_expected}")
                if a.piece_cid != piece_cid_v1:
                    reasons.append("allocation piece CID mismatch")
                if a.term_min < 0 or a.term_max < a.term_min:
                    reasons.append(
                        f"allocation term range is invalid: {a.term_min}..{a.term_max}"
                    )
                else:
                    if duration < a.term_min:
                        reasons.append(
                            f"deal duration below allocation term_min: {duration} < {a.term_min}"
                        )
                    if duration > a.term_max:
                        reasons.append(
                            f"deal duration above allocation term_max: {duration} > {a.term_max}"
                        )

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

    allocation_counts = Counter(
        c.allocation_id for c in out if c.allocation_id is not None
    )
    duplicate_allocation_ids = {
        allocation_id
        for allocation_id, count in allocation_counts.items()
        if count > 1
    }
    for c in out:
        if c.allocation_id in duplicate_allocation_ids:
            extra = f"duplicate allocation_id in csv batch: {c.allocation_id}"
            c.file_reject_reason = (
                f"{c.file_reject_reason}; {extra}"
                if c.file_reject_reason
                else extra
            )
    return out


def write_candidates_csv(path: Path, candidates: List[Candidate], provider: str, provider_id: int, deal_client: str, client_id: int, duration: int) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "csv_row_no", "deal_id", "deal_client", "allocation_client_id", "provider", "provider_id", "duration",
            "data_cid", "piece_cid_v1", "piece_cid_v2", "piece_size", "car_size", "car_url",
            "allocation_id", "alloc_term_min", "alloc_term_max", "alloc_expiration", "start_epoch", "file_reject_reason",
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
                    "start_epoch": c.start_epoch if c.start_epoch is not None else "\\N",
                    "file_reject_reason": c.file_reject_reason or "\\N",
                    "data_json": c.data_json(),
                    "ddo_v1_json": c.ddo_v1_json(provider, duration),
                    "retrieval_v1_json": c.retrieval_v1_json(),
                    "pdp_v1_json": "null",
                }
            )


def conflict_predicate(alias: str = "s") -> str:
    # Used in multiple places. Deal id and allocation id conflicts are global.
    # Piece CID conflicts are provider-scoped so the same piece can be imported
    # for multiple miners/providers as independent replicas.
    return f"""
  EXISTS (SELECT 1 FROM market_mk20_deal d WHERE d.id = {alias}.deal_id)
  OR EXISTS (SELECT 1 FROM market_mk20_pipeline_waiting w WHERE w.id = {alias}.deal_id)
  OR EXISTS (
    SELECT 1 FROM market_mk20_deal d
    WHERE d.ddo_v1 #>> '{{ddo,allocation_id}}' = {alias}.allocation_id::TEXT
       OR (
            d.ddo_v1 #>> '{{ddo,provider}}' = {alias}.provider
            AND (
              d.piece_cid_v2 = {alias}.piece_cid_v2
              OR d.data #>> '{{piece_cid,/}}' = {alias}.piece_cid_v2
            )
          )
  )
  OR EXISTS (
    SELECT 1
    FROM market_mk20_pipeline_waiting w
    JOIN market_mk20_deal d ON d.id = w.id
    WHERE d.ddo_v1 #>> '{{ddo,allocation_id}}' = {alias}.allocation_id::TEXT
       OR (
            d.ddo_v1 #>> '{{ddo,provider}}' = {alias}.provider
            AND (
              d.piece_cid_v2 = {alias}.piece_cid_v2
              OR d.data #>> '{{piece_cid,/}}' = {alias}.piece_cid_v2
            )
          )
  )
  OR EXISTS (
    SELECT 1
    FROM market_mk20_download_pipeline p
    LEFT JOIN market_mk20_deal d ON d.id = p.id
    WHERE p.id = {alias}.deal_id
       OR (
            d.ddo_v1 #>> '{{ddo,provider}}' = {alias}.provider
            AND p.piece_cid_v2 = {alias}.piece_cid_v2
          )
  )
  OR EXISTS (
    SELECT 1 FROM market_mk20_pipeline p
    WHERE p.id = {alias}.deal_id
       OR p.allocation_id = {alias}.allocation_id
       OR (
            p.sp_id = {alias}.provider_id
            AND (p.piece_cid_v2 = {alias}.piece_cid_v2 OR p.piece_cid = {alias}.piece_cid_v1)
          )
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
    stage_table = validate_stage_table(stage_table)
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
  start_epoch BIGINT,
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

ALTER TABLE {stage_table}
  ADD COLUMN IF NOT EXISTS start_epoch BIGINT;

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
  start_epoch BIGINT,
  file_reject_reason TEXT,
  data_json JSONB,
  ddo_v1_json JSONB,
  retrieval_v1_json JSONB,
  pdp_v1_json JSONB
) ON COMMIT DROP;

\\copy mk20_import_tmp (csv_row_no, deal_id, deal_client, allocation_client_id, provider, provider_id, duration, data_cid, piece_cid_v1, piece_cid_v2, piece_size, car_size, car_url, allocation_id, alloc_term_min, alloc_term_max, alloc_expiration, start_epoch, file_reject_reason, data_json, ddo_v1_json, retrieval_v1_json, pdp_v1_json) FROM {sql_literal(csv_path)} WITH (FORMAT csv, HEADER true, NULL '\\N')

INSERT INTO {stage_table} (
  batch_name, csv_row_no, deal_id, deal_client, allocation_client_id, provider, provider_id, duration,
  data_cid, piece_cid_v1, piece_cid_v2, piece_size, car_size, car_url,
  allocation_id, alloc_term_min, alloc_term_max, alloc_expiration, start_epoch,
  file_reject_reason, data_json, ddo_v1_json, retrieval_v1_json, pdp_v1_json
)
SELECT
  {sql_literal(batch_name)}, csv_row_no, deal_id, deal_client, allocation_client_id, provider, provider_id, duration,
  data_cid, piece_cid_v1, piece_cid_v2, piece_size, car_size, car_url,
  allocation_id, alloc_term_min, alloc_term_max, alloc_expiration, start_epoch,
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
SET db_reject_reason = 'allocation already exists in market_mk20_deal'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1 FROM market_mk20_deal d
    WHERE d.ddo_v1 #>> '{{ddo,allocation_id}}' = s.allocation_id::TEXT
  );

UPDATE {stage_table} s
SET db_reject_reason = 'same-provider piece already exists in market_mk20_deal'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1 FROM market_mk20_deal d
    WHERE d.ddo_v1 #>> '{{ddo,provider}}' = s.provider
      AND (
        d.piece_cid_v2 = s.piece_cid_v2
        OR d.data #>> '{{piece_cid,/}}' = s.piece_cid_v2
      )
  );

UPDATE {stage_table} s
SET db_reject_reason = 'allocation already in market_mk20_pipeline_waiting'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1
    FROM market_mk20_pipeline_waiting w
    JOIN market_mk20_deal d ON d.id = w.id
    WHERE d.ddo_v1 #>> '{{ddo,allocation_id}}' = s.allocation_id::TEXT
  );

UPDATE {stage_table} s
SET db_reject_reason = 'same-provider piece already in market_mk20_pipeline_waiting'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1
    FROM market_mk20_pipeline_waiting w
    JOIN market_mk20_deal d ON d.id = w.id
    WHERE d.ddo_v1 #>> '{{ddo,provider}}' = s.provider
      AND (
        d.piece_cid_v2 = s.piece_cid_v2
        OR d.data #>> '{{piece_cid,/}}' = s.piece_cid_v2
      )
  );

UPDATE {stage_table} s
SET db_reject_reason = 'deal id already exists in market_mk20_download_pipeline'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1 FROM market_mk20_download_pipeline p
    WHERE p.id = s.deal_id
  );

UPDATE {stage_table} s
SET db_reject_reason = 'same-provider piece already exists in market_mk20_download_pipeline'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1
    FROM market_mk20_download_pipeline p
    JOIN market_mk20_deal d ON d.id = p.id
    WHERE d.ddo_v1 #>> '{{ddo,provider}}' = s.provider
      AND p.piece_cid_v2 = s.piece_cid_v2
  );

UPDATE {stage_table} s
SET db_reject_reason = 'allocation already exists in market_mk20_pipeline'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1 FROM market_mk20_pipeline p
    WHERE p.allocation_id = s.allocation_id
  );

UPDATE {stage_table} s
SET db_reject_reason = 'same-provider piece already exists in market_mk20_pipeline'
WHERE batch_name = {sql_literal(batch_name)}
  AND file_reject_reason IS NULL AND db_reject_reason IS NULL
  AND EXISTS (
    SELECT 1 FROM market_mk20_pipeline p
    WHERE p.sp_id = s.provider_id
      AND (p.piece_cid_v2 = s.piece_cid_v2 OR p.piece_cid = s.piece_cid_v1)
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

SELECT 'valid_sample' AS section, csv_row_no, deal_id, deal_client, allocation_client_id, provider, provider_id,
       piece_cid_v1, piece_cid_v2, allocation_id, alloc_expiration, start_epoch, car_url
FROM {stage_table}
WHERE batch_name = {sql_literal(batch_name)} AND valid
ORDER BY csv_row_no
LIMIT 20;
"""


def generate_insert_sql(
    batch_name: str,
    stage_table: str,
    limit: Optional[int],
    run_id: str,
    chain_head: int,
    start_before_allocation_expiration_epochs: int,
    expected_seal_runway_epochs: int,
) -> str:
    stage_table = validate_stage_table(stage_table)
    limit_clause = f"LIMIT {int(limit)}" if limit is not None and limit > 0 else ""
    empty_pick_message = sql_literal(
        f"No valid picked rows for run_id={run_id} batch={batch_name}"
    )
    count_mismatch_message = sql_literal(
        f"Insert count mismatch for run_id={run_id}: picked %, deal %, waiting %"
    )
    scheduling_mismatch_message = sql_literal(
        f"Unsafe StartEpoch scheduling metadata for run_id={run_id}; rerun file validation and staging"
    )
    return f"""
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- Serialize importer runs across the distributed YSQL cluster. This cooperative
-- transaction lock is released automatically on commit or rollback.
SELECT pg_advisory_xact_lock(1296778320);

CREATE TABLE IF NOT EXISTS audit_mk20_import_runs (
  run_id TEXT PRIMARY KEY,
  batch_name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  limit_rows BIGINT,
  expected_rows BIGINT,
  inserted_deals BIGINT,
  inserted_waiting BIGINT,
  chain_head BIGINT,
  start_before_allocation_expiration_epochs BIGINT,
  expected_seal_runway_epochs BIGINT,
  notes TEXT
);

ALTER TABLE audit_mk20_import_runs
  ADD COLUMN IF NOT EXISTS chain_head BIGINT;
ALTER TABLE audit_mk20_import_runs
  ADD COLUMN IF NOT EXISTS start_before_allocation_expiration_epochs BIGINT;
ALTER TABLE audit_mk20_import_runs
  ADD COLUMN IF NOT EXISTS expected_seal_runway_epochs BIGINT;

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
  start_epoch BIGINT,
  alloc_expiration BIGINT,
  car_url TEXT NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, deal_id)
);

ALTER TABLE audit_mk20_import_inserted
  ADD COLUMN IF NOT EXISTS start_epoch BIGINT;
ALTER TABLE audit_mk20_import_inserted
  ADD COLUMN IF NOT EXISTS alloc_expiration BIGINT;

-- Refuse run_id reuse. If this fails, choose a new --run-id.
INSERT INTO audit_mk20_import_runs (
  run_id, batch_name, limit_rows, chain_head,
  start_before_allocation_expiration_epochs, expected_seal_runway_epochs, notes
)
VALUES (
  {sql_literal(run_id)}, {sql_literal(batch_name)}, {sql_literal(limit) if limit else 'NULL'},
  {sql_literal(chain_head)}, {sql_literal(start_before_allocation_expiration_epochs)},
  {sql_literal(expected_seal_runway_epochs)}, {sql_literal('mk20 db importer ' + VERSION)}
);

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
    RAISE EXCEPTION {empty_pick_message};
  END IF;
END $$;

-- Fail closed if an older or modified stage row does not match this run's
-- supplied scheduling snapshot and policy. Verification after commit is not
-- a substitute for this pre-insert guard.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM picked s
    WHERE jsonb_typeof(s.ddo_v1_json #> '{{ddo,start_epoch}}') IS DISTINCT FROM 'number'
       OR s.ddo_v1_json #>> '{{ddo,start_epoch}}' IS DISTINCT FROM s.start_epoch::TEXT
       OR (s.alloc_expiration > s.start_epoch) IS NOT TRUE
       OR (s.alloc_expiration - s.start_epoch = {int(start_before_allocation_expiration_epochs)}) IS NOT TRUE
       OR (s.start_epoch >= {int(chain_head)} + {int(expected_seal_runway_epochs)}) IS NOT TRUE
  ) THEN
    RAISE EXCEPTION {scheduling_mismatch_message};
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
    piece_cid_v1, piece_cid_v2, allocation_id, start_epoch, alloc_expiration, car_url
  )
  SELECT
    {sql_literal(run_id)}, {sql_literal(batch_name)}, p.csv_row_no, p.deal_id, p.deal_client, p.allocation_client_id,
    p.provider, p.provider_id, p.piece_cid_v1, p.piece_cid_v2, p.allocation_id,
    p.start_epoch, p.alloc_expiration, p.car_url
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
    RAISE EXCEPTION {count_mismatch_message}, p, d, w;
  END IF;
END $$;

COMMIT;

SELECT 'insert_result' AS section, run_id, batch_name, expected_rows, inserted_deals, inserted_waiting,
       chain_head, start_before_allocation_expiration_epochs, expected_seal_runway_epochs
FROM audit_mk20_import_runs
WHERE run_id = {sql_literal(run_id)};
"""


def generate_verify_sql(batch_name: str, stage_table: str, run_id: str) -> str:
    stage_table = validate_stage_table(stage_table)
    return f"""
-- Verification for mk20 importer run_id={run_id}
\nSELECT 'run_manifest' AS section,
       run_id, batch_name, created_at, limit_rows, expected_rows,
       inserted_deals, inserted_waiting, chain_head,
       start_before_allocation_expiration_epochs, expected_seal_runway_epochs, notes
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
WHERE i.run_id = {sql_literal(run_id)}
  AND d.client IS DISTINCT FROM i.deal_client;

SELECT 'bad_provider_or_allocation' AS problem, d.id,
       d.ddo_v1 #>> '{{ddo,provider}}' AS provider,
       i.provider AS expected_provider,
       d.ddo_v1 #>> '{{ddo,allocation_id}}' AS allocation_id,
       i.allocation_id AS expected_allocation_id
FROM market_mk20_deal d
JOIN audit_mk20_import_inserted i ON i.deal_id = d.id
WHERE i.run_id = {sql_literal(run_id)}
  AND (
    d.ddo_v1 #>> '{{ddo,provider}}' IS DISTINCT FROM i.provider
    OR d.ddo_v1 #>> '{{ddo,allocation_id}}' IS DISTINCT FROM i.allocation_id::TEXT
  );

SELECT 'bad_piece_or_url' AS problem, d.id,
       d.piece_cid_v2, i.piece_cid_v2 AS expected_piece_cid_v2,
       d.data #>> '{{source_http,urls,0,url}}' AS url,
       i.car_url AS expected_url
FROM market_mk20_deal d
JOIN audit_mk20_import_inserted i ON i.deal_id = d.id
WHERE i.run_id = {sql_literal(run_id)}
  AND (
    d.piece_cid_v2 IS DISTINCT FROM i.piece_cid_v2
    OR d.data #>> '{{piece_cid,/}}' IS DISTINCT FROM i.piece_cid_v2
    OR d.data #>> '{{source_http,urls,0,url}}' IS DISTINCT FROM i.car_url
  );

SELECT 'bad_ddo_start_epoch' AS problem, d.id,
       d.ddo_v1 #>> '{{ddo,start_epoch}}' AS ddo_start_epoch,
       i.start_epoch AS audited_start_epoch
FROM market_mk20_deal d
JOIN audit_mk20_import_inserted i ON i.deal_id = d.id
WHERE i.run_id = {sql_literal(run_id)}
  AND (
    jsonb_typeof(d.ddo_v1 #> '{{ddo,start_epoch}}') IS DISTINCT FROM 'number'
    OR d.ddo_v1 #>> '{{ddo,start_epoch}}' IS DISTINCT FROM i.start_epoch::TEXT
  );

SELECT 'bad_allocation_start_order' AS problem, i.deal_id,
       i.alloc_expiration, i.start_epoch
FROM audit_mk20_import_inserted i
WHERE i.run_id = {sql_literal(run_id)}
  AND (
    i.alloc_expiration IS NULL
    OR i.start_epoch IS NULL
    OR i.alloc_expiration <= i.start_epoch
  );

SELECT 'bad_allocation_start_buffer' AS problem, i.deal_id,
       i.alloc_expiration, i.start_epoch,
       r.start_before_allocation_expiration_epochs AS expected_buffer
FROM audit_mk20_import_inserted i
JOIN audit_mk20_import_runs r ON r.run_id = i.run_id
WHERE i.run_id = {sql_literal(run_id)}
  AND (i.alloc_expiration - i.start_epoch)
      IS DISTINCT FROM r.start_before_allocation_expiration_epochs;

SELECT 'insufficient_audited_start_runway' AS problem, i.deal_id,
       i.start_epoch, r.chain_head, r.expected_seal_runway_epochs
FROM audit_mk20_import_inserted i
JOIN audit_mk20_import_runs r ON r.run_id = i.run_id
WHERE i.run_id = {sql_literal(run_id)}
  AND (
    i.start_epoch IS NULL
    OR r.chain_head IS NULL
    OR r.expected_seal_runway_epochs IS NULL
    OR i.start_epoch < r.chain_head + r.expected_seal_runway_epochs
  );

SELECT 'duplicate_piece_cid_v2_in_market_mk20_deal' AS problem,
       d.ddo_v1 #>> '{{ddo,provider}}' AS provider,
       d.piece_cid_v2,
       COUNT(*) AS count
FROM market_mk20_deal d
WHERE EXISTS (
  SELECT 1
  FROM audit_mk20_import_inserted i
  WHERE i.run_id = {sql_literal(run_id)}
    AND i.provider = d.ddo_v1 #>> '{{ddo,provider}}'
    AND i.piece_cid_v2 = d.piece_cid_v2
)
GROUP BY d.ddo_v1 #>> '{{ddo,provider}}', d.piece_cid_v2
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
       d.ddo_v1 #>> '{{ddo,start_epoch}}' AS ddo_start_epoch,
       i.start_epoch, i.alloc_expiration,
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
        OR p.direct_piece_activation_manifest #>> '{{CID,/}}' = i.piece_cid_v1
        OR p.direct_piece_activation_manifest #>> '{{CID,/}}' = i.piece_cid_v2
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

DO $$
DECLARE n BIGINT;
BEGIN
  SELECT COUNT(*) INTO n FROM rollback_ids;
  IF n = 0 THEN
    RAISE EXCEPTION 'Refusing rollback: run_id has no audited inserted rows';
  END IF;
END $$;

CREATE TEMP TABLE rollback_ref_ids AS
SELECT DISTINCT unnest(p.ref_ids) AS ref_id
FROM market_mk20_download_pipeline p
JOIN rollback_ids r ON r.deal_id = p.id;

DELETE FROM market_mk20_pipeline_waiting w USING rollback_ids r WHERE w.id = r.deal_id;
DELETE FROM market_mk20_pipeline p USING rollback_ids r WHERE p.id = r.deal_id;
DELETE FROM market_mk20_download_pipeline p USING rollback_ids r WHERE p.id = r.deal_id;
DELETE FROM parked_piece_refs pr
USING rollback_ref_ids rr
WHERE pr.ref_id = rr.ref_id
  AND NOT EXISTS (
    SELECT 1
    FROM market_mk20_download_pipeline remaining
    WHERE rr.ref_id = ANY(remaining.ref_ids)
  );
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
SELECT 'run_policy' AS section, run_id, batch_name, chain_head,
       start_before_allocation_expiration_epochs, expected_seal_runway_epochs
FROM audit_mk20_import_runs
WHERE run_id = {sql_literal(run_id)};

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
      OR p.direct_piece_activation_manifest #>> '{{CID,/}}' = i.piece_cid_v1
      OR p.direct_piece_activation_manifest #>> '{{CID,/}}' = i.piece_cid_v2
      OR p.direct_piece_activation_manifest #>> '{{VerifiedAllocationKey,ID}}' = i.allocation_id::TEXT
 )
WHERE i.run_id = {sql_literal(run_id)};

SELECT i.csv_row_no, i.deal_id, i.piece_cid_v1, i.piece_cid_v2, i.allocation_id,
       i.start_epoch, i.alloc_expiration,
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
    cmd = shlex.split(psql_cmd)
    if not cmd:
        raise ValueError("--psql-cmd must contain an executable")
    cmd.extend(["-v", "ON_ERROR_STOP=1", "-f", str(sql_file)])
    eprint(f"+ {shlex.join(cmd)}")
    subprocess.run(cmd, check=True)


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
    ap.add_argument(
        "--chain-head",
        required=True,
        type=int,
        help="chain height snapshot used for StartEpoch safety validation",
    )
    ap.add_argument(
        "--start-before-allocation-expiration-epochs",
        default=DEFAULT_START_BEFORE_ALLOCATION_EXPIRATION_EPOCHS,
        type=int,
        help="derive StartEpoch this many epochs before allocation expiration",
    )
    ap.add_argument(
        "--expected-seal-runway-epochs",
        default=DEFAULT_EXPECTED_SEAL_RUNWAY_EPOCHS,
        type=int,
        help="minimum runway required between chain head and derived StartEpoch",
    )
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

    try:
        safe_label(args.batch_name, "batch name")
        validate_stage_table(args.stage_table)
        if args.run_id is not None:
            safe_label(args.run_id, "run id")
        validate_provider_id_address(args.provider, args.provider_id)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.client_id <= 0 or args.provider_id <= 0:
        raise SystemExit("--client-id and --provider-id must be positive")
    if args.piece_size <= 0 or args.piece_size & (args.piece_size - 1):
        raise SystemExit("--piece-size must be a positive power of two")
    if args.duration <= 0:
        raise SystemExit("--duration must be positive")
    if args.chain_head <= 0:
        raise SystemExit("--chain-head must be positive")
    if args.start_before_allocation_expiration_epochs <= 0:
        raise SystemExit(
            "--start-before-allocation-expiration-epochs must be positive"
        )
    if args.expected_seal_runway_epochs <= 0:
        raise SystemExit("--expected-seal-runway-epochs must be positive")
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be a positive integer when specified")
    if args.no_db and args.execute:
        raise SystemExit("--no-db cannot be combined with --execute")
    for option, value in [
        ("--deal-client", args.deal_client),
        ("--provider", args.provider),
    ]:
        if not value or value != value.strip() or "\x00" in value:
            raise SystemExit(f"{option} must be non-empty and contain no surrounding whitespace or NUL")
    if args.execute:
        if not args.ack_db_direct:
            raise SystemExit("Refusing production insert: --execute requires --ack-db-direct")
        if args.limit is None and not args.allow_full_batch:
            raise SystemExit("Refusing full batch insert: specify --limit, or add --allow-full-batch intentionally")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    id_time_ms = args.id_time_ms if args.id_time_ms is not None else int(time.time() * 1000)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    run_id = args.run_id or f"{args.batch_name}_{timestamp}"
    safe_label(run_id, "run id")

    allocations = read_allocations(args.allocations)
    allocations_by_piece: Dict[str, List[Allocation]] = defaultdict(list)
    matching_allocations = 0
    for a in allocations:
        # Allocation JSONs may contain multiple miners/providers. Only allocations
        # for this client, provider, and piece size should participate in the
        # per-piece lookup. Cross-provider duplicates are normal replicas.
        if a.client == args.client_id and a.miner == args.provider_id and a.piece_size == args.piece_size:
            matching_allocations += 1
            if a.piece_cid:
                allocations_by_piece[a.piece_cid].append(a)

    candidates = read_csv_candidates(
        args.csv, args.batch_name, id_time_ms, allocations_by_piece,
        args.client_id, args.provider_id, args.piece_size, args.duration,
        args.chain_head, args.start_before_allocation_expiration_epochs,
        args.expected_seal_runway_epochs,
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
    insert_sql.write_text(
        generate_insert_sql(
            args.batch_name,
            args.stage_table,
            args.limit,
            run_id,
            args.chain_head,
            args.start_before_allocation_expiration_epochs,
            args.expected_seal_runway_epochs,
        ),
        encoding="utf-8",
    )
    verify_sql.write_text(generate_verify_sql(args.batch_name, args.stage_table, run_id), encoding="utf-8")
    rollback_sql.write_text(generate_rollback_sql(args.batch_name, run_id), encoding="utf-8")
    observe_sql.write_text(generate_observe_sql(args.batch_name, run_id), encoding="utf-8")

    print(f"mk20_db_importer version: {VERSION}")
    print(f"run id: {run_id}")
    print(f"allocations: {len(allocations)} records")
    print(f"matching allocations for provider/client/size: {matching_allocations}")
    print(f"csv rows: {total}")
    print(f"file-valid candidates: {file_valid}")
    print(f"file-rejected candidates: {file_rejected}")
    print(f"deal client: {args.deal_client}")
    print(f"allocation client id: {args.client_id}")
    print(f"chain head snapshot: {args.chain_head}")
    print(
        "start before allocation expiration: "
        f"{args.start_before_allocation_expiration_epochs} epochs"
    )
    print(f"expected seal runway: {args.expected_seal_runway_epochs} epochs")
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
