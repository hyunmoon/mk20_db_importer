# Curio MK20 DB Importer

`mk20_db_importer.py` is an SP/operator-side tool for validating Filecoin Plus allocations and enqueueing Curio MK20 DDO deals directly in Curio's database.

It is intended for an operator who already has a provider CSV and a freshly fetched allocation list. It does not sign with, load, or require a client private key or wallet file. It does require the public MK20 deal-client address because Curio stores that value in `market_mk20_deal.client`.

> **Production warning:** `--execute` bypasses Curio's normal API and sanitize path and writes directly to production tables. Treat it as an operator-controlled production action. Start with one row, verify it, observe downstream progress, and scale only after the result is clean.

## What production insertion writes

For a normal MK20 DDO HTTP-source deal, Curio's HTTP submission path initially writes:

1. `market_mk20_deal`
2. `market_mk20_pipeline_waiting`

This importer follows that initial shape. Curio's background pipeline consumes the waiting row and creates downstream download, MK20 pipeline, parked-piece, and sector rows. The importer does not directly create those downstream production rows.

A production run also creates or updates importer-owned audit data:

- `audit_mk20_import_runs`
- `audit_mk20_import_inserted`

## Three execution levels

The importer has three distinct operating levels. Generating SQL is not the same as safely executing it.

| Level | Flags | Database effect |
| --- | --- | --- |
| **A. File-only validation** | Include `--no-db`; do not use `--execute` | Does not connect to YugabyteDB/PostgreSQL. Validates input files and generates the validated CSV plus stage, insert, verify, observe, and rollback SQL artifacts. Production tables are untouched. |
| **B. DB staging / dry run** | Omit both `--no-db` and `--execute` | Executes only the generated stage SQL. It loads or updates the importer staging table, performs current DB-side conflict checks, and does **not** insert production MK20 deal rows. |
| **C. Production direct-DB insertion** | Include `--execute --ack-db-direct` | Stages first, then executes the generated insert SQL in a serializable transaction. This bypasses Curio's normal API/sanitize path. |

> **Generated SQL warning:** `--no-db` still writes an `insert.sql` file. If `--no-db` is run without `--limit`, that file may represent the entire valid batch. Do not manually execute a generated `insert.sql` unless its intended scope, batch, run ID, and selected rows have been verified. Prefer invoking the importer with the explicit production safeguards.

`--limit` limits only the rows selected by the production insert SQL; it does not reduce file validation or DB staging. A production run without `--limit` additionally requires `--allow-full-batch`.

## Safety and concurrency model

- Production insertion requires both `--execute` and `--ack-db-direct`.
- An unlimited production insertion additionally requires `--allow-full-batch`.
- The insert transaction uses `SERIALIZABLE` isolation.
- Before the final conflict-check-and-insert critical section, every importer insert run obtains the same transaction-scoped advisory lock.
- The lock relies on YugabyteDB advisory-lock support. The documented target is YugabyteDB v2025.1 or later with `ysql_yb_enable_advisory_locks=true`.
- The advisory lock is **cooperative only**: it serializes importer instances using this same lock, but it does not serialize Curio/API processes or unrelated writers that do not take the lock.
- Advisory-lock, serialization, and SQL failures are surfaced to the operator through `ON_ERROR_STOP=1` and checked subprocess execution. They are not automatically retried.
- Insert SQL repeats conflict checks after acquiring the lock and aborts on an empty selection or count mismatch.
- Each production run has a unique `run_id` and an audit manifest.
- Database commands are launched as an argument vector rather than through a shell.
- Batch and run labels accept 1-128 characters, including spaces, but reject CR, LF, NUL, `/`, and `\`. Stage-table names use a separate restricted SQL-identifier grammar and must begin with `audit_mk20_import_`.
- The importer explicitly writes `market_mk20_deal.created_at = now()` to preserve the actual transaction timestamp in non-UTC sessions.

Because unrelated Curio writers do not cooperate with the advisory lock, a clean staging result is not a guarantee that database state will remain unchanged before insertion. Operators must treat a surfaced transaction failure as a failed run and inspect state before retrying with a new run ID.

## Public repository and secret hygiene

Do not commit operational inputs or generated artifacts. They can expose CAR URLs, client addresses, CIDs, database locations, and generated SQL.

Keep these out of the repository:

- provider CSV files
- allocation JSON files
- `mk20-import-out/`
- generated `*.sql`
- generated `*.validated.csv`
- database credentials and shell history containing them
- private keys, wallet files, API tokens, internal hostnames/IPs, and logs

Use a local wrapper, `.pgpass`, environment variables, or a secret manager for database access. The included `.gitignore` is intentionally broad.

## Required inputs

### Provider CSV

The CSV must have exactly these columns, in this order:

```text
data_cid,piece_cid_v1,pcidv2,piece_size,car_size,car_url
```

`pcidv2` is the FRC-0069 PieceCIDv2 stored in `market_mk20_deal.piece_cid_v2` and `data.piece_cid`. `piece_cid_v1` is the Filecoin allocation piece CID. The CAR URL path must end with `<piece_cid_v1>.car`.

### Active allocations JSON

A recommended way to produce the allocation input is to query Lotus immediately before staging:

```bash
lotus filplus list-allocations --json <client-address> > allocations.json
```

The importer accepts all of these forms:

- a top-level JSON list, including the direct output of the Lotus command above
- `{"allocations": [...]}` or `{"Allocations": [...]}`
- `{"allocations": {"123": {...}}}`, `{"Allocations": {"123": {...}}}`, and equivalent object forms
- older flat mappings keyed by allocation ID

For example, Lotus output in this form is accepted directly without conversion:

```json
[
  {
    "allocationid": 125422425,
    "client": 3662041,
    "expiration": 6429145,
    "miner": 3199233,
    "piececid": {
      "/": "baga6ea4..."
    },
    "piecesize": 34359738368,
    "termmax": 5256000,
    "termmin": 518400
  }
]
```

Common field-name casing variants are accepted for allocation ID, client, miner/provider, piece CID, piece size, term bounds, and expiration. Object mappings may supply the allocation ID as the mapping key.

Fetch allocations from current chain state immediately before staging. The importer trusts the supplied JSON; it does not query or refresh chain state itself.

## File-side validation

Before any database operation, the importer validates the CLI configuration, allocation JSON, and every CSV row.

CLI and allocation validation includes:

- `--client-id` and `--provider-id` must be positive.
- `--provider` must be an `f0...` or `t0...` ID address whose encoded actor ID equals `--provider-id`.
- `--piece-size` must be a positive power of two.
- `--duration` must be positive.
- `--limit`, when present, must be positive.
- `--no-db --execute` is rejected.
- Every allocation ID must be positive.
- Allocation lookup is restricted to the requested client, provider/miner, and piece size.
- Each CSV piece must resolve to exactly one matching allocation in the supplied allocation JSON.
- The CSV piece CID and allocation piece CID must match.
- Deal duration must be within the allocation's valid `term_min` / `term_max` range, and an invalid allocation term range is rejected.

Piece and source validation includes:

- PieceCIDv2 must be a canonical lowercase base32 CIDv1.
- It must use the raw codec and the PieceCIDv2 multihash.
- Its digest, tree height, commitment root, and encoded lengths must be structurally valid.
- It is decoded and its commitment root is converted to PieceCIDv1.
- The derived PieceCIDv1 must equal CSV `piece_cid_v1`.
- Its derived padded size must equal CSV `piece_size`.
- Padding validation mirrors `go-fil-commcid`: padding must be strictly less than half of the unpadded tree capacity.
- CSV `piece_size` must also equal the configured `--piece-size`.
- `car_size` must be positive.
- `car_url` must be an absolute HTTP(S) URL, include a hostname, contain no embedded username/password, and have the exact `<piece_cid_v1>.car` basename.
- `data_cid`, PieceCIDv1, PieceCIDv2, and CAR URL must be non-empty.
- Duplicate CSV PieceCIDv1 values, PieceCIDv2 values, and resolved allocation IDs are rejected for all affected rows in the batch.

The importer intentionally does **not** require `car_size` to equal the PieceCIDv2 raw payload size. Those values describe different input properties and equality is not enforced.

## DB-side conflict checks

DB staging marks file-valid rows invalid when relevant current state already exists. Checks cover:

- deal ID and allocation ID conflicts
- same-provider PieceCID conflicts in `market_mk20_deal` and the waiting/download/MK20 pipelines
- waiting-queue, download-pipeline, and MK20-pipeline participation
- `sectors_sdr_initial_pieces` matches by provider plus PieceCIDv1, PieceCIDv2, direct activation manifest CID, or verified allocation ID

Piece duplicates are provider-scoped so replicas for different providers are not rejected solely because they share a piece. Allocation ID conflicts are global.

## Identity fields

These values are intentionally separate:

- `--deal-client`: the public MK20 deal-client address stored in `market_mk20_deal.client`; commonly the address that would be supplied to `sptool --wallet`
- `--client-id`: numeric DataCap allocation client actor ID used for allocation validation
- `--provider`: provider/miner Filecoin ID address (`f0...` or `t0...`) stored in DDO JSON
- `--provider-id`: numeric provider/miner actor ID; it must equal the ID encoded by `--provider` (for example, `f03199233` pairs with `3199233`)

Do not pass a private key or wallet file.

## Recommended operating workflow

Use placeholders and local secret handling:

```bash
SCRIPT=./mk20_db_importer.py
CSV=/path/to/provider-deals.csv
ALLOC=/path/to/allocations.json

BATCH_NAME=my-batch-name
DEAL_CLIENT=<public-client-wallet-address>
ALLOCATION_CLIENT_ID=<numeric-client-actor-id>
PROVIDER=<provider-ID-address>
PROVIDER_ID=<numeric-provider-actor-id>
PIECE_SIZE=34359738368
DURATION=5256000

YSQL_CURIO='ysqlsh ...'
```

### 1. File-only validation

This command does not connect to the database. It validates the complete input and generates artifacts. `--limit 1` scopes the generated insert SQL to the planned canary; it does not limit validation.

```bash
RUN_ID="${BATCH_NAME}_filecheck_$(date +%Y%m%d_%H%M%S)"

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
  --limit 1 \
  --no-db
```

Review the summary and validated CSV. Production tables are untouched. Do not manually execute the generated insert SQL merely because it came from a `--no-db` run.

### 2. DB staging / dry run

Omit both `--no-db` and `--execute`. This runs the stage SQL, loads `audit_mk20_import_stage`, and evaluates current DB conflicts without inserting production MK20 rows.

```bash
RUN_ID="${BATCH_NAME}_dbcheck_$(date +%Y%m%d_%H%M%S)"

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
  --limit 1 \
  --psql-cmd "$YSQL_CURIO"
```

If the stage table predates the current schema, rerun once with `--reset-stage-table`. That flag is restricted to an `audit_mk20_import_*` table; it does not target production MK20 tables.

### 3. Review staging results

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

Only rows with `valid = TRUE` are eligible for production selection. Insert SQL repeats the conflict predicate inside the production transaction.

### 4. Create a backup or snapshot

Create a version-compatible external backup or operator-approved internal snapshot before production insertion. The following is an example for a Curio schema search path; adapt schema names to the deployment. Keep identifiers short to avoid PostgreSQL's 63-byte identifier truncation.

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

### 5. Execute a one-row production canary

Use a new run ID and insert one row:

```bash
RUN_ID="${BATCH_NAME}_canary1_$(date +%Y%m%d_%H%M%S)"

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
  --limit 1
```

Verify immediately:

```bash
$YSQL_CURIO -v ON_ERROR_STOP=1 -f "mk20-import-out/${BATCH_NAME}.${RUN_ID}.verify.sql"
```

Within the run manifest, `expected_rows`, `inserted_deals`, and `inserted_waiting` must agree. `inserted_audit_count` and `deal_rows` should match the inserted run.

`waiting_rows`, `download_pipeline_rows`, and `mk20_pipeline_rows` are live pipeline observations. They may differ as Curio consumes waiting rows and moves imported deals through the pipeline, including immediately after insertion.

Every problem query must return zero rows.

Then observe progression:

```bash
$YSQL_CURIO -v ON_ERROR_STOP=1 -f "mk20-import-out/${BATCH_NAME}.${RUN_ID}.observe.sql"
```

Expected downstream progression is:

```text
market_mk20_pipeline_waiting
  -> market_mk20_download_pipeline / market_mk20_pipeline
  -> sectors_sdr_initial_pieces
```

Temporary waiting caused by Curio scheduling, backpressure, or configured limiters is not automatically an importer failure when verification is otherwise clean. Confirm expected progress and operational health before scaling.

### 6. Scale gradually

No fixed chunk size is universally safe. Choose it from current queue depth, download capacity, sealing throughput, allocation deadlines, failure rate, and message-submission health.

A conservative progression is:

1. 1-row canary
2. 10 or 100 rows
3. 500, 1000, or another operator-selected chunk
4. the full remainder only after prior runs verify clean and show acceptable progression

For each chunk, use a new `--run-id`, run the importer with the selected positive `--limit`, then run that run's `verify.sql` and `observe.sql`.

Example 100-row step:

```bash
RUN_ID="${BATCH_NAME}_chunk100_$(date +%Y%m%d_%H%M%S)"

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
$YSQL_CURIO -v ON_ERROR_STOP=1 -f "mk20-import-out/${BATCH_NAME}.${RUN_ID}.observe.sql"
```

For the full valid remainder, omit `--limit` and explicitly add `--allow-full-batch`:

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
```

Use that mode only when processing the entire currently valid remainder is intentional. Verify and observe the full-remainder run just as you would any chunk:

```bash
$YSQL_CURIO -v ON_ERROR_STOP=1 -f "mk20-import-out/${BATCH_NAME}.${RUN_ID}.verify.sql"
$YSQL_CURIO -v ON_ERROR_STOP=1 -f "mk20-import-out/${BATCH_NAME}.${RUN_ID}.observe.sql"
```

## Capacity and deadline planning

These are different constraints:

- **Allocation expiration** is the chain epoch by which the allocation must be successfully claimed through sector activation.
- **Deal duration** must satisfy the allocation's `term_min` and `term_max`; it is not the remaining time until allocation expiration.
- **Local sealing completion** means local PC1/PC2/C2 work has progressed, but it does not prove that the allocation was claimed.
- **Successful on-chain sector activation / allocation claim** requires the relevant chain message to land successfully before allocation expiration.

Merely finishing local PC1/PC2/C2 work is not sufficient. Plan from current chain epoch to each allocation's expiration using actual sustained throughput and a substantial reserve for download delays, queue/backpressure, failed sectors, retries, message batching, and on-chain submission failures.

Illustrative capacity calculation only:

- 32,109 × 32 GiB ≈ 1003.4 TiB
- at approximately 48 TiB/day of sustained sealing throughput, raw processing time is approximately 20.9 days

The 48 TiB/day figure is an example assumption, not importer performance and not guaranteed throughput. Do not adopt a fixed “X days before expiration is safe” rule. Recalculate from current chain epoch, the relevant allocation expirations, observed end-to-end throughput, and an operator-selected safety margin.

## Verification and duplicate checks

Each production run's `verify.sql` checks audit/deal/waiting counts, persisted client/provider/allocation/piece/URL values, same-provider PieceCIDv2 duplicates, and allocation-ID duplicates.

To check importer-created rows across a batch:

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
  WHERE d.ddo_v1 #>> '{ddo,allocation_id}' IN (
    SELECT allocation_id::TEXT
    FROM imported
  )
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

This should return zero rows for importer-created records. Historical records outside importer manifests require separate assessment.

## Rollback

> **Early-stage recovery only:** automatic rollback is intended before imported pieces reach `sectors_sdr_initial_pieces`.

Each production run generates a rollback file:

```bash
$YSQL_CURIO -v ON_ERROR_STOP=1 -f "mk20-import-out/${BATCH_NAME}.${RUN_ID}.rollback.sql"
```

The rollback is anchored to `audit_mk20_import_inserted`. It deletes waiting, download, MK20 pipeline, and deal rows by audited deal ID. Before deletion, its sector blocker check identifies imported material for the same provider using, where applicable:

- PieceCIDv1
- PieceCIDv2
- direct piece activation manifest CID
- verified allocation ID

It also refuses an empty audit manifest and preserves parked-piece references still used by another download pipeline.

If any imported piece has reached `sectors_sdr_initial_pieces`, automatic rollback refuses to proceed. Manual operational recovery is then required. Audit rows are retained for traceability after a successful early rollback.

## Operational monitoring

Use the generated `observe.sql` for a run-specific view. Broader queue monitoring can include:

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

Keep these until the batch is sealed and audited:

- `audit_mk20_import_runs`
- `audit_mk20_import_inserted`
- the relevant `audit_mk20_import_stage` rows
- backup/snapshot data
- generated validated CSV, verify, observe, and rollback files

Remove temporary operator-created snapshot or isolation tables only after restoration is no longer needed and results have been verified. Do not discard audit manifests while traceability is required.

## Development verification

Run the dependency-free checks with:

```bash
python3 -m py_compile mk20_db_importer.py
python3 -m py_compile test_mk20_db_importer.py
python3 -m unittest -v
git diff --check
```

The PieceCID tests use official FRC-0069/`go-fil-commcid` reference vectors for 508-byte, empty 32 GiB, and 1016-byte payloads. GitHub Actions runs the same compile, unit-test, and diff checks on Python 3.9 and 3.13 for pull requests and pushes to `main` or `codex/**`.

## Limitations

- The importer targets the currently verified Curio MK20 DDO schema and YugabyteDB v2025.1+ advisory-lock path. Reinspect source and schema before using it with another Curio or database version.
- The advisory lock coordinates cooperating importer instances only; it does not block unrelated Curio/API writers.
- The importer does not query chain state, confirm current epoch, monitor allocation consumption, or prove on-chain activation. It trusts the supplied allocation JSON.
- It records allocation expiration for audit output but does not reject a row based on current epoch or calculate deadline safety.
- It does not enforce equality between CAR size and PieceCIDv2 raw payload size.
- It does not manage Curio backpressure, sealing capacity, or on-chain message submission.
- It does not replace normal Curio monitoring, backups, or operator judgment.
