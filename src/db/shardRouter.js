import { createHash } from 'node:crypto';

/**
 * Application-level shard routing.
 *
 *   shard key (customer_id) --md5--> uint32 --mod B--> virtual bucket (0..B-1) --bucket map--> physical shard
 *
 * The number of virtual buckets B is fixed for the lifetime of the cluster, so a key's bucket
 * never changes. Rebalancing (adding a shard) only edits the bucket -> shard map and moves the
 * rows of the reassigned buckets; every row stores its bucket in `orders.shard_bucket` for that.
 */
export class ShardRouter {
  /**
   * @param {object} opts
   * @param {number} opts.shardCount       number of physical shards
   * @param {number} [opts.virtualBuckets]  fixed number of logical buckets
   * @param {number[]} [opts.bucketMap]     explicit bucket -> shard assignment (used after a rebalance)
   */
  constructor({ shardCount, virtualBuckets = 1024, bucketMap } = {}) {
    if (!Number.isInteger(shardCount) || shardCount < 1) throw new Error('shardCount must be a positive integer');
    if (!Number.isInteger(virtualBuckets) || virtualBuckets < shardCount) {
      throw new Error('virtualBuckets must be an integer >= shardCount');
    }
    this.shardCount = shardCount;
    this.virtualBuckets = virtualBuckets;

    if (bucketMap) {
      if (bucketMap.length !== virtualBuckets || bucketMap.some((s) => !Number.isInteger(s) || s < 0 || s >= shardCount)) {
        throw new Error('bucketMap must assign every bucket to a valid shard');
      }
      this.bucketToShard = Int16Array.from(bucketMap);
    } else {
      // Default: contiguous, equal-sized bucket ranges per shard.
      this.bucketToShard = Int16Array.from({ length: virtualBuckets }, (_, bucket) =>
        Math.floor((bucket * shardCount) / virtualBuckets),
      );
    }
  }

  bucketFor(shardKey) {
    if (typeof shardKey !== 'string' || shardKey.length === 0) {
      throw new TypeError('shard key must be a non-empty string');
    }
    const digest = createHash('md5').update(shardKey, 'utf8').digest();
    return digest.readUInt32BE(0) % this.virtualBuckets;
  }

  /** @returns {{ bucket: number, shardId: number }} */
  route(shardKey) {
    const bucket = this.bucketFor(shardKey);
    return { bucket, shardId: this.bucketToShard[bucket] };
  }

  shardFor(shardKey) {
    return this.route(shardKey).shardId;
  }

  describe() {
    const ranges = [];
    for (let bucket = 0; bucket < this.virtualBuckets; bucket += 1) {
      const shardId = this.bucketToShard[bucket];
      const last = ranges.at(-1);
      if (last && last.shardId === shardId && last.to === bucket - 1) last.to = bucket;
      else ranges.push({ shardId, from: bucket, to: bucket });
    }
    return { shardKey: 'customer_id', hash: 'md5', virtualBuckets: this.virtualBuckets, shardCount: this.shardCount, ranges };
  }
}
