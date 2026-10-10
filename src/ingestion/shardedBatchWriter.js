import { performance } from 'node:perf_hooks';
import { isTransientDbError } from '../utils/errors.js';
import { withRetry } from '../utils/retry.js';
import { metrics } from '../metrics.js';

/**
 * Routes validated orders to their shard and writes them in batches.
 *
 * Keeps one buffer per shard; a buffer is flushed as one transaction once it reaches
 * `batchSize`. Awaiting the flush inside `add()` is what applies backpressure to the parser,
 * so memory stays bounded at roughly shardCount * batchSize rows regardless of file size.
 */
export class ShardedBatchWriter {
  constructor({ router, repo, jobId, batchSize, logger, onRejected, retry = { retries: 4, baseDelayMs: 250 } }) {
    this.router = router;
    this.repo = repo;
    this.jobId = jobId;
    this.batchSize = batchSize;
    this.logger = logger;
    this.onRejected = onRejected;
    this.retry = retry;
    this.buffers = new Map();
    this.nextBatchNo = new Map();
    this.stats = { inserted: 0, updated: 0, unchanged: 0, duplicateRows: 0, dbRejected: 0, batches: 0, batchesReplayed: 0, shards: {} };
  }

  async add(order) {
    const { shardId, bucket } = this.router.route(order.customerId);
    let buffer = this.buffers.get(shardId);
    if (!buffer) {
      buffer = new Map();
      this.buffers.set(shardId, buffer);
    }
    // A repeated order_id inside one batch would make ON CONFLICT DO UPDATE fail
    // ("cannot affect row a second time"). Last occurrence wins, like it would across batches.
    if (buffer.delete(order.orderId)) this.stats.duplicateRows += 1;
    buffer.set(order.orderId, { ...order, bucket });

    if (buffer.size >= this.batchSize) await this.flushShard(shardId);
  }

  async flushAll() {
    await Promise.all([...this.buffers.keys()].map((shardId) => this.flushShard(shardId)));
  }

  async flushShard(shardId) {
    const buffer = this.buffers.get(shardId);
    if (!buffer || buffer.size === 0) return;
    this.buffers.set(shardId, new Map());

    const orders = [...buffer.values()];
    const batchNo = this.nextBatchNo.get(shardId) ?? 0;
    this.nextBatchNo.set(shardId, batchNo + 1);

    const started = performance.now();
    const outcome = await withRetry(() => this.repo.writeBatch(shardId, orders, { jobId: this.jobId, batchNo }), {
      ...this.retry,
      isRetryable: isTransientDbError,
      onRetry: (err, { attempt, delayMs }) =>
        this.logger?.warn({ err, jobId: this.jobId, shardId, batchNo, attempt, delayMs }, 'batch write failed, retrying'),
    });
    const seconds = (performance.now() - started) / 1000;
    metrics.batchWriteSeconds.observe({ shard: String(shardId) }, seconds);

    this.#record(shardId, orders.length, outcome);
    this.logger?.debug(
      { jobId: this.jobId, shardId, batchNo, rows: orders.length, ms: Math.round(seconds * 1000), replayed: outcome.alreadyCommitted },
      'batch committed',
    );

    for (const { order, error } of outcome.rejectedRows) {
      await this.onRejected?.({ lineNumber: order.lineNumber, stage: 'database', errors: [error], raw: order });
    }
  }

  #record(shardId, rows, outcome) {
    const shard = (this.stats.shards[shardId] ??= { rows: 0, batches: 0 });
    shard.rows += rows;
    shard.batches += 1;
    this.stats.batches += 1;
    if (outcome.alreadyCommitted) this.stats.batchesReplayed += 1;
    this.stats.inserted += outcome.inserted;
    this.stats.updated += outcome.updated;
    this.stats.unchanged += outcome.unchanged;
    this.stats.dbRejected += outcome.rejected ?? outcome.rejectedRows.length;
  }
}
