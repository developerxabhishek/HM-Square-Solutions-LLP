-- Applied to EVERY shard database. All shards share one schema; the application decides
-- which shard a row lives in (see src/db/shardRouter.js).

CREATE TABLE IF NOT EXISTS orders (
    order_id          VARCHAR(64)    PRIMARY KEY,
    customer_id       VARCHAR(64)    NOT NULL,
    order_date        TIMESTAMPTZ    NOT NULL,
    order_amount      NUMERIC(14, 2) NOT NULL CHECK (order_amount >= 0),
    status            VARCHAR(32)    NOT NULL,
    -- Virtual bucket (0..SHARD_VIRTUAL_BUCKETS-1) of customer_id. Lets a rebalance move whole
    -- buckets between shards with "WHERE shard_bucket BETWEEN x AND y".
    shard_bucket      SMALLINT       NOT NULL,
    -- Lineage: which ingestion job last wrote this row (jobs live in the meta database).
    ingestion_job_id  UUID,
    created_at        TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ    NOT NULL DEFAULT now()
) WITH (fillfactor = 90); -- leave room on each page so status updates can be HOT updates

-- Serves "orders of a customer, newest first" (GET /orders?customerId=) with keyset pagination.
-- customer_id is also the shard key, so this query always hits exactly one shard.
CREATE INDEX IF NOT EXISTS orders_customer_date_idx
    ON orders (customer_id, order_date DESC, order_id DESC);

-- Per-shard ledger of committed batches. A batch's rows and its ledger entry are committed in
-- the same transaction, so a retried job skips batches that already landed.
CREATE TABLE IF NOT EXISTS ingested_batches (
    job_id        UUID        NOT NULL,
    batch_no      INTEGER     NOT NULL,
    row_count     INTEGER     NOT NULL,
    inserted      INTEGER     NOT NULL,
    updated       INTEGER     NOT NULL,
    unchanged     INTEGER     NOT NULL,
    rejected      INTEGER     NOT NULL,
    committed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, batch_no)
);
