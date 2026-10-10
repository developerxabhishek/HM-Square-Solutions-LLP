import { createPool, ping, withTransaction } from './pool.js';

/** Owns one connection pool per physical shard and exposes shard-addressed helpers. */
export class ShardedDatabase {
  constructor({ shardUrls, poolMax, statementTimeoutMs, logger }) {
    this.shards = shardUrls.map((url, shardId) => ({
      shardId,
      pool: createPool(url, { name: `shard-${shardId}`, max: poolMax, statementTimeoutMs, logger }),
    }));
  }

  get shardCount() {
    return this.shards.length;
  }

  pool(shardId) {
    const shard = this.shards[shardId];
    if (!shard) throw new RangeError(`unknown shard ${shardId}`);
    return shard.pool;
  }

  query(shardId, text, params) {
    return this.pool(shardId).query(text, params);
  }

  transaction(shardId, fn) {
    return withTransaction(this.pool(shardId), fn);
  }

  /** Runs the same query on every shard in parallel (scatter-gather). */
  async queryAll(text, params) {
    return Promise.all(this.shards.map(async ({ shardId, pool }) => ({ shardId, result: await pool.query(text, params) })));
  }

  async health() {
    return Promise.all(
      this.shards.map(async ({ shardId, pool }) => {
        try {
          return { shardId, ok: true, latencyMs: await ping(pool) };
        } catch (err) {
          return { shardId, ok: false, error: err.message };
        }
      }),
    );
  }

  async close() {
    await Promise.all(this.shards.map(({ pool }) => pool.end()));
  }
}
