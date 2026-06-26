# Curio MK20 DB Importer

`mk20_db_importer.py` is a conservative SP/operator-side tool for enqueueing Curio MK20 DDO deals directly into Curio DB after validating a provider CSV against an active Filecoin Plus allocation list.

It is intended for cases where the SP/operator has already received a provider deal CSV and an independently fetched active allocation list, but does not want to depend on the data provider running `sptool mk20-client deal` with a wallet.

This tool does **not** sign with, load, or require a client wallet private key. It still requires the public MK20 deal client address string because Curio stores that value in `market_mk20_deal.client`.

## What it inserts

For a normal MK20 DDO HTTP-source deal, Curio's HTTP submit path initially writes only:

1. `market_mk20_deal`
2. `market_mk20_pipeline_waiting`

Curio's own background pipeline then consumes `market_mk20_pipeline_waiting` and creates downstream rows such as parked piece refs, download pipeline rows, and MK20 pipeline rows. This importer intentionally inserts only the initial deal row and waiting-queue row. It must not directly insert downstream pipeline tables.

## Safety model

The tool is intentionally hard to run by accident:

- Default mode is dry-run/staging only.
- Production insert requires both `--execute` and `--ack-db-direct`.
- Full-batch insert additionally requires `--allow-full-batch`.
- Each production insert gets a `run_id` and writes audit rows to `audit_mk20_import_runs` and `audit_mk20_import_inserted`.
- Per-run `verify.sql`, `observe.sql`, and `rollback.sql` files are generated.
- Rollback SQL refuses to proceed if imported pieces have already reached `sectors_sdr_initial_pieces`.
- Insert SQL rechecks conflicts at insert time.
- The importer explicitly writes `market_mk20_deal.created_at = now()` to avoid schemas where the default timestamp expression is offset-shifted in non-UTC sessions.

## Public repo / secret hygiene

Do not commit real operational data. The following can contain sensitive operational details such as car URLs, client addresses, CIDs, DB paths, and generated SQL:

- provider CSV files
- allocation JSON files
- `mk20-import-out/`
- generated `*.sql`
- generated `*.validated.csv`
- shell history containing database credentials
- log files

Use a local shell wrapper, `.pgpass`, environment variables, or your secret manager for DB access. Do not put passwords, private keys, API tokens, hostnames, or internal IP addresses into this repository.

The included `.gitignore` is intentionally broad to reduce the chance of accidentally publishing runtime artifacts.

## Required inputs

### Provider CSV

The CSV must have exactly these columns:

```text
data_cid,piece_cid_v1,pcidv2,piece_size,car_size,car_url
```

`piece_cid_v1` is the Filecoin allocation piece CID and must match the basename of `car_url` as `<piece_cid_v1>.car`.

`pcidv2` is the piece CID value stored in `market_mk20_deal.piece_cid_v2` and in `data.piece_cid`.

### Active allocations JSON

The allocation list can be a list or object whose values contain these fields, with common casing variants accepted:

- allocation id: `allocationid`, `allocation_id`, `ID`, or `id`
- client actor id: `client` or `Client`
- miner/provider actor id: `miner`, `provider`, or `Provider`
- piece CID: `piececid`, `piece_cid`, `Data`, or `data`
- piece size: `piecesize`, `piece_size`, `Size`, or `size`
- term min/max: `termmin`, `term_min`, `TermMin`, `termmax`, `term_max`, `TermMax`
- expiration: `expiration` or `Expiration`

Fetch this from chain state immediately before staging so stale or already-consumed allocations are not used.

## Identity fields

These are deliberately separate:

- `--deal-client`: MK20 deal client address string stored in `market_mk20_deal.client`; usually the public wallet address that would have been used as `sptool --wallet`.
- `--client-id`: numeric DataCap allocation client actor id used for allocation validation.
- `--provider`: provider/miner address stored in DDO JSON.
- `--provider-id`: numeric provider/miner actor id used for allocation and sector-table validation.

Do not pass a private key. Do not pass a wallet file.

## Recommended workflow

Set local variables in your shell. Use placeholder values below; do not commit your real values.

```bash
SCRIPT=./mk20_db_importer.py
CSV=/path/to/provider-deals.csv
ALLOC=/path/to/active-allocations.json

BATCH_NAME=my-batch-name
DEAL_CLIENT=<public-client-wallet-address>
ALLOCATION_CLIENT_ID=<numeric-client-actor-id>
PROVIDER=<provider-address>
PROVIDER_ID=<numeric-provider-actor-id>
PIECE_SIZE=34359738368
DURATION=5256000

# Prefer a local wrapper script or .pgpass. Do not commit credentials.
YSQL_CURIO='/path/to/ysqlsh -h <db-host> -p <db-port> -U <db-user> -d <db-name>'
```

### 1. File-only validation

This does not connect to DB.

```bash
python3 "$SCRIPT" \
  --allocations "$ALLOC" \
  --csv "$CSV" \
  --batch-name "$BATCH_NAME" \
  --out-dir mk20-import-out \
  --provider "$PROVIDER" \
  --provider-id "$PROVIDER_ID" \
  --deal-client "$DEAL_CLIENT" \
  --client-id "$ALLOCATION_CLIENT_ID" \
  --piece-size "$PIECE_SIZE" \
  --duration "$DURATION" \
  --replace-stage \
  --no-db
```

Review the file validation summary. Missing active allocations and duplicate active allocations are rejected.

### 2. DB dry-run staging

This loads `audit_mk20_import_stage` and performs DB-side duplicate checks. It does not insert production deal rows.

```bash
RUN_ID="${BATCH_NAME}_dryrun_$(date +%Y%m%d_%H%M%S)"

python3 "$SCRIPT" \
  --allocations "$ALLOC" \
  --csv "$CSV" \
  --batch-name "$BATCH_NAME" \
  --run-id "$RUN_ID" \
  --out-dir mk20-import-out \
  --provider "$PROVIDER" \
  --provider-id "$PROVIDER_ID" \
  --deal-client "$DEAL_CLIENT" \
  --client-id "$ALLOCATION_CLIENT_ID" \
  --piece-size "$PIECE_SIZE" \
  --duration "$DURATION" \
  --replace-stage \
  --psql-cmd "$YSQL_CURIO"
```

If an old stage table schema exists from a previous script version, rerun once with:

```bash
  --reset-stage-table
```

Only use `--reset-stage-table` for the staging/audit stage table; it does not touch production MK20 tables.

### 3. Review staging summary

```bash
$YSQL_CURIO -c "
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE valid) AS valid,
  COUNT(*) FILTER (WHERE NOT valid) AS rejected
FROM audit_mk20_import_stage
WHERE batch_name = '$BATCH_NAME';

SELECT
  COALESCE(file_reject_reason, db_reject_reason, 'valid') AS reason,
  COUNT(*) AS count
FROM audit_mk20_import_stage
WHERE batch_name = '$BATCH_NAME'
GROUP BY 1
ORDER BY count DESC, reason;
"
```

The `valid` rows are candidates not currently known to Curio by deal id, piece CID, allocation id, waiting queue, download pipeline, MK20 pipeline, or SDR initial pieces.

### 4. Create a backup/snapshot before production insert

External dumps are best when you have a version-compatible client. If that is not available, create short-name DB-internal snapshots before production insert. Keep names short to avoid PostgreSQL's identifier-length truncation.

```bash
BTAG="b$(date +%m%d_%H%M%S)"

$YSQL_CURIO -v ON_ERROR_STOP=1 <<SQL
BEGIN;

CREATE TABLE curio.audit_${BTAG}_deal AS
SELECT * FROM curio.market_mk20_deal;

CREATE TABLE curio.audit_${BTAG}_wait AS
SELECT * FROM curio.market_mk20_pipeline_waiting;

CREATE TABLE curio.audit_${BTAG}_down AS
SELECT * FROM curio.market_mk20_download_pipeline;

CREATE TABLE curio.audit_${BTAG}_pipe AS
SELECT * FROM curio.market_mk20_pipeline;

CREATE TABLE curio.audit_${BTAG}_sdr AS
SELECT * FROM curio.sectors_sdr_initial_pieces;

COMMIT;

SELECT '${BTAG}' AS backup_tag;
SQL
```

### 5. PoC insert

Start with a very small insert.

```bash
RUN_ID="${BATCH_NAME}_poc10_$(date +%Y%m%d_%H%M%S)"

python3 "$SCRIPT" \
  --allocations "$ALLOC" \
  --csv "$CSV" \
  --batch-name "$BATCH_NAME" \
  --run-id "$RUN_ID" \
  --out-dir mk20-import-out \
  --provider "$PROVIDER" \
  --provider-id "$PROVIDER_ID" \
  --deal-client "$DEAL_CLIENT" \
  --client-id "$ALLOCATION_CLIENT_ID" \
  --piece-size "$PIECE_SIZE" \
  --duration "$DURATION" \
  --replace-stage \
  --psql-cmd "$YSQL_CURIO" \
  --execute \
  --ack-db-direct \
  --limit 10
```

Verify immediately:

```bash
$YSQL_CURIO -v ON_ERROR_STOP=1 -f "mk20-import-out/${BATCH_NAME}.${RUN_ID}.verify.sql"
```

All problem queries must return zero rows.

Observe movement:

```bash
$YSQL_CURIO -v ON_ERROR_STOP=1 -f "mk20-import-out/${BATCH_NAME}.${RUN_ID}.observe.sql"
```

Expected path:

```text
market_mk20_pipeline_waiting
  -> market_mk20_download_pipeline / market_mk20_pipeline
  -> sectors_sdr_initial_pieces after Curio scheduling progresses
```

Backpressure or custom Curio limiters may leave rows in waiting for a while. That is not an importer failure if verify is clean.

### 6. Scale up gradually

After the PoC reaches the expected downstream path, scale by chunks. Each run restages the CSV and excludes rows already inserted by previous runs.

Example 100-row run:

```bash
RUN_ID="${BATCH_NAME}_real_100_$(date +%Y%m%d_%H%M%S)"

python3 "$SCRIPT" \
  --allocations "$ALLOC" \
  --csv "$CSV" \
  --batch-name "$BATCH_NAME" \
  --run-id "$RUN_ID" \
  --out-dir mk20-import-out \
  --provider "$PROVIDER" \
  --provider-id "$PROVIDER_ID" \
  --deal-client "$DEAL_CLIENT" \
  --client-id "$ALLOCATION_CLIENT_ID" \
  --piece-size "$PIECE_SIZE" \
  --duration "$DURATION" \
  --replace-stage \
  --psql-cmd "$YSQL_CURIO" \
  --execute \
  --ack-db-direct \
  --limit 100

$YSQL_CURIO -v ON_ERROR_STOP=1 -f "mk20-import-out/${BATCH_NAME}.${RUN_ID}.verify.sql"
```

Repeat with `--limit 500`, `--limit 1000`, or another size appropriate for your waiting-queue and pipeline capacity.

For the final remainder, only after smaller chunks verify clean:

```bash
RUN_ID="${BATCH_NAME}_remaining_$(date +%Y%m%d_%H%M%S)"

python3 "$SCRIPT" \
  --allocations "$ALLOC" \
  --csv "$CSV" \
  --batch-name "$BATCH_NAME" \
  --run-id "$RUN_ID" \
  --out-dir mk20-import-out \
  --provider "$PROVIDER" \
  --provider-id "$PROVIDER_ID" \
  --deal-client "$DEAL_CLIENT" \
  --client-id "$ALLOCATION_CLIENT_ID" \
  --piece-size "$PIECE_SIZE" \
  --duration "$DURATION" \
  --replace-stage \
  --psql-cmd "$YSQL_CURIO" \
  --execute \
  --ack-db-direct \
  --allow-full-batch

$YSQL_CURIO -v ON_ERROR_STOP=1 -f "mk20-import-out/${BATCH_NAME}.${RUN_ID}.verify.sql"
```

## Duplicate checks

The generated verify SQL checks duplicate participation for the current run. To check all importer-created rows for a batch:

```bash
$YSQL_CURIO <<SQL
WITH imported AS (
  SELECT deal_id, run_id, piece_cid_v2, allocation_id
  FROM audit_mk20_import_inserted
  WHERE batch_name = '$BATCH_NAME'
),
dup_piece AS (
  SELECT d.piece_cid_v2
  FROM market_mk20_deal d
  WHERE d.ddo_v1 #>> '{ddo,provider}' = '$PROVIDER'
  GROUP BY d.piece_cid_v2
  HAVING COUNT(*) > 1
),
dup_alloc AS (
  SELECT d.ddo_v1 #>> '{ddo,allocation_id}' AS allocation_id
  FROM market_mk20_deal d
  WHERE d.ddo_v1 #>> '{ddo,provider}' = '$PROVIDER'
    AND d.ddo_v1 #>> '{ddo,allocation_id}' IS NOT NULL
  GROUP BY d.ddo_v1 #>> '{ddo,allocation_id}'
  HAVING COUNT(*) > 1
)
SELECT
  i.run_id,
  i.deal_id,
  i.piece_cid_v2,
  i.allocation_id,
  dp.piece_cid_v2 IS NOT NULL AS duplicate_piece,
  da.allocation_id IS NOT NULL AS duplicate_allocation
FROM imported i
LEFT JOIN dup_piece dp ON dp.piece_cid_v2 = i.piece_cid_v2
LEFT JOIN dup_alloc da ON da.allocation_id = i.allocation_id::TEXT
WHERE dp.piece_cid_v2 IS NOT NULL
   OR da.allocation_id IS NOT NULL
ORDER BY i.run_id, i.deal_id
LIMIT 50;
SQL
```

This should return zero rows for importer-created rows. Existing historical duplicates outside importer runs may still exist and should be assessed separately.

## Rollback

Each production run generates a rollback SQL file. It is intended only for early-stage rollback before imported pieces reach SDR initial pieces.

```bash
$YSQL_CURIO -v ON_ERROR_STOP=1 -f "mk20-import-out/${BATCH_NAME}.${RUN_ID}.rollback.sql"
```

If any imported row has reached `sectors_sdr_initial_pieces`, the rollback SQL refuses to run. At that point manual operational recovery is required.

## Operational monitoring

```bash
watch -n 30 "$YSQL_CURIO -c \"
SELECT COUNT(*) AS waiting FROM market_mk20_pipeline_waiting;

SELECT
  COUNT(*) AS total_pipeline,
  COUNT(*) FILTER (WHERE downloaded IS NOT TRUE) AS not_downloaded,
  COUNT(*) FILTER (WHERE downloaded IS TRUE AND after_commp IS NOT TRUE) AS downloaded_not_commp,
  COUNT(*) FILTER (WHERE after_commp IS TRUE AND aggregated IS NOT TRUE) AS after_commp_not_aggregated,
  COUNT(*) FILTER (WHERE aggregated IS TRUE AND sector IS NULL) AS aggregated_no_sector,
  COUNT(*) FILTER (WHERE sector IS NOT NULL) AS has_sector
FROM market_mk20_pipeline;
\""
```

## Cleanup guidance

Keep these until the batch is fully sealed and audited:

- `audit_mk20_import_runs`
- `audit_mk20_import_inserted`
- current `audit_mk20_import_stage`
- backup snapshot tables
- generated `verify.sql`, `observe.sql`, and `rollback.sql`

Temporary tables you intentionally created for manual queue isolation can be removed after restoration and verification. Do not delete audit manifest tables unless you no longer need traceability.

## Limitations

- This tool targets the Curio MK20 DDO schema verified for the current source flow. Reinspect Curio source and schema before using it with a new Curio version.
- It does not verify chain state directly. It trusts the provided active allocation JSON and validates it against CSV and current Curio DB state.
- It does not manage Curio backpressure. Large imports can create large waiting queues if Curio intake is paused or rate-limited.
- It does not replace normal Curio monitoring, logs, or operational judgment.
