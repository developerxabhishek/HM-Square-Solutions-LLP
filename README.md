# Orders Ingestion Service

A Node.js service that takes an orders CSV file and does the following:

1. Stores the file in **Google Cloud Storage**, authenticated with **Application Default Credentials (ADC)**.
2. Parses and validates it as a **stream**. The file is never fully loaded into memory.
3. Writes valid rows to a **sharded PostgreSQL** cluster. Rows go in as **batched, transactional upserts**, routed by `customer_id`.
4. Records every rejected row with its line number and reason, and exposes job status, order lookups, health checks and Prometheus metrics.

Stack: Node.js 20+ (ESM), Express 5, `pg`, `@google-cloud/storage`, `busboy`, `pino` and `prom-client`. CSV parsing is built in.

---

## Contents

- [Architecture](#architecture)
- [Quick start: one command, no installs](#quick-start-one-command-no-installs)
- [Running with your own PostgreSQL](#running-with-your-own-postgresql)
- [Google Cloud: ADC setup](#google-cloud-adc-setup)
- [API](#api)
- [Sharding strategy](#sharding-strategy)
- [Schema](#schema)
- [Ingestion pipeline and performance](#ingestion-pipeline-and-performance)
- [Reliability: retries and idempotency](#reliability-retries-and-idempotency)
- [Error handling and logging](#error-handling-and-logging)
- [Tests](#tests)
- [Design decisions and trade-offs](#design-decisions-and-trade-offs)
- [Project layout](#project-layout)

---

## Architecture

```
                POST /upload-orders (multipart)
                          |
   +----------------------v-----------------------+
   | API (Express)                                |
   |  1. stream body -> temp file + SHA-256       |
   |  2. same SHA already ingested? -> 200 (dup)  |
   |  3. upload to GCS  gs://bucket/orders/uploads/<sha256>.csv  (ADC)
   |  4. INSERT ingestion_jobs (queued)  -> 202 + /jobs/:id
   +----------------------+-----------------------+
                          | job row (meta DB)
   +----------------------v-----------------------+       +------------------+
   | Worker(s)  (in-process or `npm run start:worker`)    |  meta Postgres   |
   |  claim job: UPDATE ... FOR UPDATE SKIP LOCKED  <-----> |  ingestion_jobs  |
   |  GCS read stream                              |       |  ingestion_errors|
   |   -> LineSplitter -> parseCsvLine             |       +------------------+
   |   -> validateOrder ----invalid----> RejectSink (batched) --^
   |   -> ShardRouter(customer_id)                 |
   |   -> per-shard buffer (1000 rows)             |
   |   -> 1 transaction per batch:                 |
   |        ledger row + unnest() upsert           |
   +-------+---------------+---------------+-------+
           |               |               |
     +-----v----+    +-----v----+    +-----v----+
     | shard 0  |    | shard 1  |    | shard 2  |   (separate PostgreSQL servers)
     | orders   |    | orders   |    | orders   |
     +----------+    +----------+    +----------+
```

The API and the workers communicate only through the job table and GCS. A worker reads the file back from GCS, not from the API host's disk, so the two can scale and fail independently.

---

## Quick start: one command, no installs

Requires only Node.js 20 or later. PostgreSQL is downloaded as a dev dependency and run locally, so you don't install it yourself.

```bash
npm install
npm run dev            # then open http://localhost:3000
```

`npm run dev` (`scripts/dev.js`) does the following:
- starts a local PostgreSQL on port 54329, with its data in `.dev-postgres/`
- creates the meta database and 3 shard databases
- applies the migrations
- generates `sample-data/orders.csv` (10,000 rows, about 2% deliberately invalid)
- starts storage (see below)
- starts the API, the worker and the web UI, restarting on code changes

Ctrl+C stops everything. Data is kept between runs; `npm run dev -- --reset` starts from empty.

**Storage modes**

| Mode | When | What happens |
|---|---|---|
| Real GCS | `GCS_BUCKET` is set in `.env` | Uploads go to `gs://<bucket>`, authenticated with ADC (see [ADC setup](#google-cloud-adc-setup)). |
| GCS emulator (default) | no bucket configured | [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) runs on `127.0.0.1:4443`. It is downloaded once into `.dev-tools/` and its checksum is verified. The app runs the **same `GcsStorage` code and Google client library** as production; only `GCS_API_ENDPOINT` points to the emulator. |
| Local folder | `npm run dev -- --local` | Files are copied to `.local-storage/`, with no GCS API involved. |

## Running with your own PostgreSQL

Requirements: Node.js ≥ 20 and PostgreSQL ≥ 13 (one meta database plus N shard databases).

```bash
npm install
cp .env.example .env          # point META_DATABASE_URL / SHARD_DATABASE_URLS at your databases
npm run migrate               # applies migrations/meta to meta, migrations/shard to every shard
npm start                     # API + embedded worker on :3000
# optional: more workers in other processes or machines
RUN_EMBEDDED_WORKER=false npm start  &  npm run start:worker
```

For offline development without a GCP project, set `STORAGE_DRIVER=local`. Files then go to `LOCAL_STORAGE_DIR` instead of GCS. This is only for testing; the default and intended path is GCS.

The shards can be separate databases on one server (fine for development) or separate servers (production). The routing code is the same either way.

---

## Google Cloud: ADC setup

`src/storage/gcsStorage.js` creates the client with **no credentials**: `new Storage({ projectId })`. The library then resolves Application Default Credentials in this order:

| Environment | How credentials are found | What you do |
|---|---|---|
| Developer machine | `~/.config/gcloud/application_default_credentials.json` | `gcloud auth application-default login` |
| GKE | Workload Identity (KSA mapped to a GSA) via the metadata server | bind the GSA to the pod's service account |
| Cloud Run / GCE | the attached service account via the metadata server | deploy with `--service-account=...` |

The repository contains no key files, and `.gitignore` blocks common credential filenames. The identity needs `roles/storage.objectCreator` and `roles/storage.objectViewer` on the bucket. Workers read the file back, so they need the viewer role.

```bash
gcloud storage buckets create gs://YOUR_BUCKET --location=asia-south1 --uniform-bucket-level-access
gcloud storage buckets add-iam-policy-binding gs://YOUR_BUCKET \
  --member="user:you@example.com" --role="roles/storage.objectAdmin"   # for local dev with your user ADC
```

Upload details:
- **Content-addressed object names.** Files are stored as `<prefix>/<sha256>.csv`, with the original filename kept in object metadata.
- **Retry-safe uploads.** Each upload uses an `ifGenerationMatch: 0` precondition, so the client library can retry it safely (`IdempotencyStrategy.RetryConditional`). An HTTP 412 means the same bytes are already there, which counts as success.
- **CRC32C checks.** Integrity is validated with CRC32C on both upload and download. Uploads over 8 MB use resumable uploads.

---

## API

### Web UI

Open **http://localhost:3000/** for a small operator page (`public/index.html`, plain HTML/JS, no build step). It uses only the endpoints below. You can:

- upload a CSV by drag-and-drop and watch the job progress live
- see rows per shard and rejected rows broken down by stage
- page through the rejected rows with their reasons
- look up orders by customer ID (single shard) or order ID (all shards)
- check the health of the meta database and each shard, with their bucket ranges

A job can be linked directly as `/#job=<jobId>`.

### Endpoints

| Method & path | Purpose |
|---|---|
| `GET /` | Web UI |
| `GET /jobs?limit=` | Recent uploads, newest first |
| `POST /upload-orders` | Upload a CSV (`multipart/form-data`, field `file`). Returns **202** and a job; with `?wait=true` it blocks up to `UPLOAD_WAIT_TIMEOUT_MS` and returns **200** with final stats. |
| `GET /jobs/:jobId` | Job status and stats (inserted/updated/unchanged/rejected, per-shard counts, duration) |
| `GET /jobs/:jobId/errors?limit=&offset=` | Rejected rows: line number, stage, reasons, raw values |
| `GET /orders/:orderId` | One order (parallel lookup across shards) |
| `GET /orders?customerId=&limit=&cursor=` | A customer's orders, newest first, keyset-paginated (single shard) |
| `GET /health` | Readiness: pings the meta database and every shard (returns 503 if any is down) |
| `GET /health/live` | Liveness |
| `GET /metrics` | Prometheus metrics |
| `GET /shards` | Current bucket → shard map |

**CSV format.** The header must contain `order_id, customer_id, order_date, order_amount, status`, in any order and any case. Extra columns are ignored. `order_amout`, the spelling used in the brief, is also accepted.

| Field | Rule |
|---|---|
| `order_id`, `customer_id` | required, ≤ 64 chars, no control characters (UUIDs or any string ID) |
| `order_date` | ISO-8601: `2024-05-01`, `2024-05-01 10:15`, `2024-05-01T10:15:00.123+05:30`. Values without an offset are treated as **UTC**. Impossible dates (`2024-02-30`) are rejected. |
| `order_amount` | non-negative decimal, ≤ 12 integer and ≤ 2 fraction digits (kept as a string, never a float) |
| `status` | one of `pending, processing, shipped, delivered, completed, cancelled, refunded, returned, failed` (case-insensitive; `canceled` is accepted) |

Example:

```bash
$ curl -s -F "file=@sample-data/orders.csv" "localhost:3000/upload-orders?wait=true"
{
  "duplicate": false,
  "jobId": "ef471b43-5818-43e3-a33f-54a54ef71d76",
  "status": "completed",
  "file": { "name": "orders.csv", "sizeBytes": 906375, "sha256": "d1755a…", "storageUri": "gs://bucket/orders/uploads/d1755a….csv" },
  "stats": {
    "totalRows": 10000, "validRows": 9800, "inserted": 9800, "updated": 0, "unchanged": 0,
    "rejectedRows": 200, "rejectedByStage": { "parse": 21, "validation": 179, "database": 0 },
    "batches": 12, "batchesReplayed": 0,
    "shards": { "0": { "rows": 3208, "batches": 4 }, "1": { "rows": 3123, "batches": 4 }, "2": { "rows": 3469, "batches": 4 } },
    "durationMs": 672
  },
  "links": { "self": "/jobs/ef47…", "errors": "/jobs/ef47…/errors" }
}

$ curl -s "localhost:3000/jobs/ef47…/errors?limit=1"
{ "errors": [ { "lineNumber": 74, "stage": "validation",
    "errors": ["status \"teleported\" is not one of: pending, processing, …"],
    "raw": { "order_id": "33d5…", "customer_id": "CUST-000883", "status": "teleported", … } } ] }
```

Upload error responses all use the shape `{ "error": { "code", "message" } }`:
`415 UNSUPPORTED_MEDIA_TYPE / UNSUPPORTED_FILE_TYPE`, `400 MISSING_FILE / EMPTY_FILE / MALFORMED_MULTIPART`, `413 FILE_TOO_LARGE`, and `502 STORAGE_UPLOAD_FAILED`. A 502 creates no job.

---

## Sharding strategy

**Application-level sharding across separate PostgreSQL databases, keyed by a hash of `customer_id`, through a fixed set of virtual buckets.**

```
customer_id ──md5──► uint32 ──mod 1024──► virtual bucket (0‥1023) ──bucket map──► physical shard
```

- **Why `customer_id`?** The main read pattern, "orders for a customer" (`GET /orders?customerId=`), then touches **exactly one shard**. A customer's orders are co-located, so per-customer aggregates and future multi-row transactions stay single-shard. Hashing spreads customers evenly: the test suite checks the spread is within ±5% on 30k keys, and the 10k sample split 3208 / 3123 / 3469.
- **Why not time-based?** Time sharding puts all of today's writes on one hot shard and spreads every customer across all shards.
- **Why not `hash(order_id)`?** It distributes perfectly, but every customer query becomes a scatter-gather across all shards.
- **Why virtual buckets?** Plain `hash % N` moves almost every key when N changes. Here the bucket count is fixed for the life of the cluster, so a key's bucket never changes. Adding a shard only reassigns some buckets in the bucket map, and only those buckets' rows move. Every row stores its `shard_bucket` so a rebalance can copy them with `WHERE shard_bucket BETWEEN x AND y`. `ShardRouter` accepts an explicit `bucketMap` for the post-rebalance state; a test checks that only the reassigned buckets change shard.
- **Routing.** `ShardRouter.route(customerId)` returns `{ bucket, shardId }`. `ShardedBatchWriter` keeps one buffer per shard, and each flushed batch goes to its own shard in one transaction. The end-to-end run checked all 9,800 inserted rows and found **0** on the wrong shard.
- **Lookup by `order_id`.** `order_id` is not the shard key, so `GET /orders/:orderId` queries all shards **in parallel**. Each query is a primary-key probe, so cost is N index lookups and latency is that of the slowest shard. That is acceptable for a handful of shards. With many shards, add an `order_id → shard` directory or encode the shard in the ID.
- **Shard topology is configuration.** `SHARD_DATABASE_URLS` is an ordered list in which position N is shard N. Jobs record the shard count they were created with, and a worker refuses (permanently) to process a job created under a different topology. That prevents silent misrouting partway through a reshard.

---

## Schema

Migrations live in `migrations/` and are applied by `npm run migrate`, which records them in a `schema_migrations` table and serializes concurrent runs with an advisory lock. Running it twice is safe.

**Each shard: `migrations/shard/001_create_orders.sql`**

```sql
orders (
  order_id         VARCHAR(64)   PRIMARY KEY,
  customer_id      VARCHAR(64)   NOT NULL,
  order_date       TIMESTAMPTZ   NOT NULL,
  order_amount     NUMERIC(14,2) NOT NULL CHECK (order_amount >= 0),
  status           VARCHAR(32)   NOT NULL,
  shard_bucket     SMALLINT      NOT NULL,     -- for rebalancing
  ingestion_job_id UUID,                       -- lineage: which upload last wrote the row
  created_at, updated_at TIMESTAMPTZ
) WITH (fillfactor = 90);
INDEX orders_customer_date_idx (customer_id, order_date DESC, order_id DESC)

ingested_batches (job_id, batch_no) PRIMARY KEY, row_count, inserted, updated, unchanged, rejected
```

- **Types.** `NUMERIC` stores money exactly, never as a float. `TIMESTAMPTZ` makes timestamps unambiguous. `order_id` is `VARCHAR(64)` instead of `UUID` because the brief allows "String / UUID", so UUIDs and business IDs such as `ORD-1001` both work.
- **Indexes.** There are only two: the PK and the composite index that serves the customer query with keyset pagination. Every extra index slows each insert, so I didn't add speculative ones. `fillfactor = 90` leaves room for HOT updates when a status changes.

**Meta database: `migrations/meta/001_create_ingestion_jobs.sql`**

- `ingestion_jobs` is the job queue and status record. `file_sha256` is UNIQUE. Partial indexes cover the queue poll (`WHERE status='queued'`) and stale-lock recovery.
- `ingestion_errors` holds rejected rows, with PK `(job_id, line_number)`, so re-processing never duplicates them.

---

## Ingestion pipeline and performance

```
GCS stream → LineSplitter (Transform) → parseCsvLine → validateOrder → ShardedBatchWriter → shard
```

- **Streaming end to end.** The upload is streamed to a temp file while being hashed, so memory stays constant. The worker reads from a GCS stream through an object-mode `Transform` inside `stream.pipeline`. Every `await` in the consumer loop (a batch flush, a reject flush) pauses the source, so memory is bounded by about *shardCount × batchSize* rows no matter how large the file is.
- **Batch inserts.** One statement per batch of 1,000 rows (`INSERT_BATCH_SIZE`), using `INSERT … SELECT FROM unnest($1::varchar[], …)`. That is 7 bind parameters whatever the batch size, so there is no 65,535-parameter limit and one stable plan.
- **Upserts.** `ON CONFLICT (order_id) DO UPDATE … WHERE (…) IS DISTINCT FROM (…)`. Re-sending identical rows writes nothing. `RETURNING (xmax = 0)` separates inserted rows from updated ones for the stats.
- **Transactions.** Each batch is one transaction on its shard that contains both the rows and the batch's ledger entry.
- **Measured.** The 10,000-row sample (~900 KB, 3 shards, PostgreSQL 18, one laptop) ingested in about **0.7 s**: 12 batch transactions instead of 10,000 single inserts.

Tunables: `INSERT_BATCH_SIZE`, `WORKER_CONCURRENCY`, `PG_POOL_MAX`.

---

## Reliability: retries and idempotency

| Layer | Mechanism |
|---|---|
| Upload | **Content hash (SHA-256)**: uploading the same bytes again returns the existing job (`200, duplicate: true`) and doesn't re-upload. Re-uploading a file whose job **failed** re-queues that job. |
| GCS | `ifGenerationMatch: 0` precondition and library auto-retry with backoff; 412 is treated as success |
| Job queue | Postgres table with `FOR UPDATE SKIP LOCKED`, so any number of workers poll without double-claiming. Failed attempts go back on the queue with exponential backoff (30 s, 60 s, …) up to `JOB_MAX_ATTEMPTS`. **Permanent** errors (bad header, empty file, topology change) fail immediately. |
| Crashed workers | Workers heartbeat (`locked_at`) every `PROGRESS_EVERY_ROWS`. A job whose heartbeat is older than `JOB_STALE_LOCK_SECONDS` is reclaimed by another worker. Completion and failure updates are guarded by `locked_by`, so a zombie worker can't overwrite the new owner. |
| Batch writes | Transient DB errors (connection loss, serialization failure, deadlock, too many connections) are retried with jittered exponential backoff. |
| Exactly-once effect | Each batch commits its rows **and** a ledger row `(job_id, batch_no)` in the same transaction on its shard. A retried job re-reads the file and skips batches that already committed, reusing their recorded counts. Combined with idempotent upserts, retries never double-apply. Verified by re-running a completed job: 12 of 12 batches replayed with no row changes. |
| Bad rows at DB level | If Postgres rejects a batch because of a row (SQLSTATE class 22/23), the batch is **bisected under savepoints** to isolate the bad rows. Those are recorded as `database`-stage rejects and the rest of the batch commits. Verified with a temporary CHECK constraint: 174 offending rows isolated, 2,826 committed. |

---

## Error handling and logging

- **Rejected rows** never fail a file. Each is stored in `ingestion_errors` with line number, stage and raw values, and is visible at `GET /jobs/:id/errors`. The stages are:
  - `parse`: wrong field count, broken quoting, line > 64 KB
  - `validation`: field rules
  - `database`: rows Postgres refused
- **File-level problems** fail the job with a clear `lastError`, for example `header is missing required column(s): order_id, …`.
- **Structured JSON logs** (pino) include a request ID (`x-request-id` is honoured or generated), job ID and worker ID. Logged events:
  - `upload received`, `storage upload started` and `storage upload finished`, with timing
  - `ingestion job queued`, `processing started`, `processing progress`, and `processing completed` with full stats
  - `processing failed`, with whether the job will be retried
  - `batch write failed, retrying`
  - each rejected row, at warn level for the first 100 per job and debug after that, so a bad file can't flood the logs
- **Metrics** at `/metrics`:
  - `orders_uploads_total{result}`
  - `orders_rows_total{outcome}`
  - `orders_jobs_finished_total{status}`
  - `orders_batch_write_seconds{shard}` (histogram)
  - `orders_job_duration_seconds`
  - Node process metrics
- **Graceful shutdown.** On SIGTERM/SIGINT the service stops accepting HTTP requests, lets in-flight jobs finish and closes the pools.

---

## Tests

```bash
npm test            # node:test, no database or GCP needed (48 tests)
```

The unit and integration tests use in-memory fakes for the repositories and storage. They cover:
- **Routing:** deterministic, evenly distributed; a rebalance moves only reassigned buckets.
- **Validation:** every field rule.
- **CSV handling:** chunk boundaries, split multi-byte UTF-8, BOM, CRLF, quoting, over-long lines.
- **Pipeline:** routing correctness, batch-size bounds, gap-free batch numbering, rejects with line numbers, in-batch duplicates, transient-error retry, ledger replay, progress callbacks, permanent errors.
- **HTTP:** 202 and duplicate handling, failed-job re-queue, 400/413/415/502 paths, temp-file cleanup, query validation, health and metrics.

The SQL was also exercised end to end against real PostgreSQL 18: migrations, 10k-row ingest, shard-placement check, upserts, pagination, ledger replay, DB-level row isolation and retry scheduling. The GCS path (`GcsStorage` with the official client library) was tested against fake-gcs-server: upload, the `ifGenerationMatch` duplicate check, and CRC32C-validated read-back. It has not been run against a real bucket, because the development account had no billing.

---

## Design decisions and trade-offs

- **CSV, not Excel.** CSV can be streamed row by row. XLSX is a zip archive that most libraries load fully into memory, which works against the streaming requirement.
- **One record per physical line.** I wrote a small RFC 4180 line parser instead of using a general CSV parser. With a multi-line-aware parser, one unbalanced quote makes the parser treat the rest of the file as a single field, silently losing every following row; I reproduced this with csv-parse. Order fields never contain newlines, so line-scoped parsing isolates each bad row and gives exact line numbers. The trade-off is that quoted fields can't contain line breaks.
- **Async processing (202 + job).** Large files would otherwise block the HTTP request and die with it. `?wait=true` gives synchronous behaviour for small files and demos.
- **Postgres as the queue instead of Redis/PubSub.** It needs no extra infrastructure, and job state is transactional alongside the data. At much higher job rates I'd switch to Pub/Sub or Cloud Tasks; `JobWorker` only depends on `claimNext/markCompleted/markFailed`.
- **Per-batch transactions, not one transaction per file.** A single transaction can't span multiple shards without two-phase commit, and even per shard it would hold locks and WAL for the whole file. Per-batch transactions plus the ledger plus idempotent upserts make every retry safe. The cost is that **a file is not atomic**: while a job runs, or after it permanently fails, some batches are visible and others are not. Re-uploading or re-queuing resumes where it stopped.
- **Last write wins** for an `order_id` repeated within a file, or in a later file. The update is skipped when nothing changed.
- **Limitation: `customer_id` must not change for an existing `order_id`.** If it did, the new row would route to a different shard and the old copy would stay on the original. Enforcing this would need a global `order_id` directory, which is the same directory that would make `GET /orders/:orderId` single-shard. I'd add it next if order-ID lookups became hot or customer re-assignment were a real case.
- **Hot customers.** Hashing by customer can't split one very large customer across shards. For order data this is rarely a problem; if it were, those customers could be salted into several buckets.
- **The meta database is a single, unsharded instance.** It holds one row per file plus the rejected rows, which is negligible volume. In production it would be a small HA Cloud SQL instance.
- **Rejected rows from a replayed batch.** If a worker dies after a batch commits but before its `database`-stage rejects were written, the ledger keeps the counts, but those particular rejected rows aren't re-recorded. Validation-stage rejects are always re-derived. I accepted this rare edge case.

**Next steps for production:**
- Cloud SQL with private IP and IAM database authentication (also ADC-based, so no DB passwords).
- A tool that copies bucket ranges between shards for online rebalancing.
- OpenTelemetry tracing.
- Signed-URL direct-to-GCS uploads, so large files never pass through the API.
- A GCS finalize-event trigger as an alternative way to enqueue jobs.

---

## Project layout

```
src/
  server.js / worker.js       entry points (API+embedded worker / standalone worker)
  container.js                composition root (dependency wiring)
  config.js                   env parsing + validation
  http/                       Express app, multipart streaming, routes, presenters
public/index.html             operator web UI (served at /)
  storage/                    GcsStorage (ADC) + LocalStorage (offline only)
  db/                         ShardRouter, ShardedDatabase, pools/transactions, migrator
  orders/ordersRepository.js  batch upsert + ledger + savepoint bisection, queries
  ingestion/                  line splitter/CSV parser, validator, batch writer, reject sink, pipeline
  jobs/                       queue repository, worker, job processor
migrations/meta, migrations/shard   SQL schema
scripts/migrate.js, scripts/generate-orders.js
test/                         node:test suites
```
