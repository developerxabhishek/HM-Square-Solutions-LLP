-- Applied to the META database only: job queue + rejected rows. Not sharded; it holds
-- one row per uploaded file and is tiny compared with the order data.

CREATE TABLE IF NOT EXISTS ingestion_jobs (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    file_name        TEXT        NOT NULL,
    -- Content hash: uploading the same bytes twice returns the existing job (idempotent upload).
    file_sha256      CHAR(64)    NOT NULL UNIQUE,
    file_size_bytes  BIGINT      NOT NULL,
    gcs_bucket       TEXT        NOT NULL,
    gcs_object       TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'queued'
                     CHECK (status IN ('queued', 'processing', 'completed', 'failed')),
    attempts         INTEGER     NOT NULL DEFAULT 0,
    max_attempts     INTEGER     NOT NULL DEFAULT 3,
    next_run_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by        TEXT,
    locked_at        TIMESTAMPTZ,
    -- Settings that decide batch boundaries; frozen at creation so retries line up with the ledger.
    batch_size       INTEGER     NOT NULL,
    shard_count      INTEGER     NOT NULL,
    stats            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ
);

-- Queue polling: only queued rows, ordered by when they become runnable.
CREATE INDEX IF NOT EXISTS ingestion_jobs_runnable_idx
    ON ingestion_jobs (next_run_at) WHERE status = 'queued';

-- Stale-lock recovery for jobs whose worker died mid-run.
CREATE INDEX IF NOT EXISTS ingestion_jobs_processing_idx
    ON ingestion_jobs (locked_at) WHERE status = 'processing';

CREATE TABLE IF NOT EXISTS ingestion_errors (
    job_id       UUID        NOT NULL REFERENCES ingestion_jobs (id) ON DELETE CASCADE,
    line_number  INTEGER     NOT NULL,
    stage        TEXT        NOT NULL CHECK (stage IN ('parse', 'validation', 'database')),
    errors       TEXT[]      NOT NULL,
    raw_record   JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, line_number)
);
