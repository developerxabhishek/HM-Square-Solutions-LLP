import { isRowDataError } from '../utils/errors.js';

// unnest() keeps the statement at 7 bind parameters no matter how many rows are in the batch,
// so there is no 65k-parameter ceiling and Postgres can reuse one plan shape.
// `xmax = 0` is true only for freshly inserted tuples, which separates inserts from updates.
// The WHERE on DO UPDATE skips rows whose content is unchanged (re-uploads don't churn tuples).
const UPSERT_ORDERS_SQL = `
  INSERT INTO orders (order_id, customer_id, order_date, order_amount, status, shard_bucket, ingestion_job_id)
  SELECT o.order_id, o.customer_id, o.order_date, o.order_amount, o.status, o.shard_bucket, $7::uuid
  FROM unnest($1::varchar[], $2::varchar[], $3::timestamptz[], $4::numeric[], $5::varchar[], $6::smallint[])
       AS o(order_id, customer_id, order_date, order_amount, status, shard_bucket)
  ON CONFLICT (order_id) DO UPDATE SET
    customer_id      = EXCLUDED.customer_id,
    order_date       = EXCLUDED.order_date,
    order_amount     = EXCLUDED.order_amount,
    status           = EXCLUDED.status,
    shard_bucket     = EXCLUDED.shard_bucket,
    ingestion_job_id = EXCLUDED.ingestion_job_id,
    updated_at       = now()
  WHERE (orders.customer_id, orders.order_date, orders.order_amount, orders.status)
        IS DISTINCT FROM (EXCLUDED.customer_id, EXCLUDED.order_date, EXCLUDED.order_amount, EXCLUDED.status)
  RETURNING (xmax = 0) AS inserted`;

const ORDER_COLUMNS = 'order_id, customer_id, order_date, order_amount, status, created_at, updated_at';

function toRow(row, shardId) {
  return {
    orderId: row.order_id,
    customerId: row.customer_id,
    orderDate: row.order_date,
    orderAmount: row.order_amount,
    status: row.status,
    shardId,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

const emptyOutcome = () => ({ inserted: 0, updated: 0, unchanged: 0, rejectedRows: [] });

function mergeOutcomes(a, b) {
  return {
    inserted: a.inserted + b.inserted,
    updated: a.updated + b.updated,
    unchanged: a.unchanged + b.unchanged,
    rejectedRows: [...a.rejectedRows, ...b.rejectedRows],
  };
}

export class OrdersRepository {
  /** @param {{ db: import('../db/shardedDatabase.js').ShardedDatabase, router: import('../db/shardRouter.js').ShardRouter }} deps */
  constructor({ db, router }) {
    this.db = db;
    this.router = router;
  }

  /**
   * Writes one batch to one shard in a single transaction together with its ledger entry.
   * If the ledger already has (jobId, batchNo) the batch committed in an earlier attempt and is skipped.
   */
  async writeBatch(shardId, orders, { jobId, batchNo }) {
    return this.db.transaction(shardId, async (client) => {
      const claim = await client.query(
        `INSERT INTO ingested_batches (job_id, batch_no, row_count, inserted, updated, unchanged, rejected)
         VALUES ($1, $2, $3, 0, 0, 0, 0)
         ON CONFLICT (job_id, batch_no) DO NOTHING`,
        [jobId, batchNo, orders.length],
      );
      if (claim.rowCount === 0) {
        const { rows } = await client.query(
          'SELECT inserted, updated, unchanged, rejected FROM ingested_batches WHERE job_id = $1 AND batch_no = $2',
          [jobId, batchNo],
        );
        return { ...rows[0], rejectedRows: [], alreadyCommitted: true };
      }

      const outcome = await this.#upsertIsolatingBadRows(client, orders, jobId);
      await client.query(
        `UPDATE ingested_batches SET inserted = $3, updated = $4, unchanged = $5, rejected = $6
         WHERE job_id = $1 AND batch_no = $2`,
        [jobId, batchNo, outcome.inserted, outcome.updated, outcome.unchanged, outcome.rejectedRows.length],
      );
      return { ...outcome, rejected: outcome.rejectedRows.length, alreadyCommitted: false };
    });
  }

  /**
   * Tries the whole batch under a savepoint. If Postgres rejects it because of a bad row
   * (data/constraint error), bisects the batch to isolate the offending rows instead of
   * failing the other 999. Infrastructure errors propagate so the caller can retry.
   */
  async #upsertIsolatingBadRows(client, orders, jobId) {
    if (orders.length === 0) return emptyOutcome();
    await client.query('SAVEPOINT batch_rows');
    try {
      const result = await client.query(UPSERT_ORDERS_SQL, [
        orders.map((o) => o.orderId),
        orders.map((o) => o.customerId),
        orders.map((o) => o.orderDate),
        orders.map((o) => o.orderAmount),
        orders.map((o) => o.status),
        orders.map((o) => o.bucket),
        jobId,
      ]);
      await client.query('RELEASE SAVEPOINT batch_rows');
      const inserted = result.rows.filter((row) => row.inserted).length;
      return { inserted, updated: result.rowCount - inserted, unchanged: orders.length - result.rowCount, rejectedRows: [] };
    } catch (err) {
      await client.query('ROLLBACK TO SAVEPOINT batch_rows');
      await client.query('RELEASE SAVEPOINT batch_rows');
      if (!isRowDataError(err)) throw err;
      if (orders.length === 1) {
        return { ...emptyOutcome(), rejectedRows: [{ order: orders[0], error: `${err.code}: ${err.message}` }] };
      }
      const middle = orders.length >>> 1;
      const left = await this.#upsertIsolatingBadRows(client, orders.slice(0, middle), jobId);
      const right = await this.#upsertIsolatingBadRows(client, orders.slice(middle), jobId);
      return mergeOutcomes(left, right);
    }
  }

  /**
   * order_id is not the shard key, so a lookup fans out to every shard in parallel.
   * Each probe is a primary-key lookup, so cost is N index probes (N = shard count).
   */
  async findById(orderId) {
    const results = await this.db.queryAll(`SELECT ${ORDER_COLUMNS} FROM orders WHERE order_id = $1`, [orderId]);
    const hit = results.find(({ result }) => result.rowCount > 0);
    return hit ? toRow(hit.result.rows[0], hit.shardId) : null;
  }

  /** Single-shard query: customer_id is the shard key. Keyset-paginated, newest first. */
  async findByCustomer(customerId, { limit = 50, cursor } = {}) {
    const shardId = this.router.shardFor(customerId);
    const params = [customerId, limit + 1];
    let keyset = '';
    if (cursor) {
      params.push(cursor.orderDate, cursor.orderId);
      keyset = 'AND (order_date, order_id) < ($3::timestamptz, $4::varchar)';
    }
    const { rows } = await this.db.query(
      shardId,
      `SELECT ${ORDER_COLUMNS} FROM orders
       WHERE customer_id = $1 ${keyset}
       ORDER BY order_date DESC, order_id DESC
       LIMIT $2`,
      params,
    );
    const page = rows.slice(0, limit).map((row) => toRow(row, shardId));
    const last = page.at(-1);
    const nextCursor = rows.length > limit && last ? { orderDate: last.orderDate.toISOString(), orderId: last.orderId } : null;
    return { shardId, orders: page, nextCursor };
  }
}
