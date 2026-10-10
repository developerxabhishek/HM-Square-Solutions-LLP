import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ShardRouter } from '../src/db/shardRouter.js';

test('routing is deterministic and within range', () => {
  const router = new ShardRouter({ shardCount: 3, virtualBuckets: 1024 });
  for (let i = 0; i < 1000; i += 1) {
    const key = `CUST-${i}`;
    const first = router.route(key);
    assert.deepEqual(router.route(key), first);
    assert.ok(first.shardId >= 0 && first.shardId < 3);
    assert.ok(first.bucket >= 0 && first.bucket < 1024);
  }
});

test('a new router instance routes identically (no hidden state)', () => {
  const a = new ShardRouter({ shardCount: 4 });
  const b = new ShardRouter({ shardCount: 4 });
  for (let i = 0; i < 200; i += 1) assert.equal(a.shardFor(`k${i}`), b.shardFor(`k${i}`));
});

test('keys spread roughly evenly across shards', () => {
  const router = new ShardRouter({ shardCount: 3 });
  const counts = [0, 0, 0];
  const keys = 30_000;
  for (let i = 0; i < keys; i += 1) counts[router.shardFor(`CUST-${String(i).padStart(6, '0')}`)] += 1;
  for (const count of counts) assert.ok(Math.abs(count - keys / 3) / (keys / 3) < 0.05, `skewed distribution: ${counts}`);
});

test('default bucket map assigns contiguous equal ranges', () => {
  const { ranges } = new ShardRouter({ shardCount: 4, virtualBuckets: 1024 }).describe();
  assert.deepEqual(ranges, [
    { shardId: 0, from: 0, to: 255 },
    { shardId: 1, from: 256, to: 511 },
    { shardId: 2, from: 512, to: 767 },
    { shardId: 3, from: 768, to: 1023 },
  ]);
});

test('an explicit bucket map moves only reassigned buckets', () => {
  const before = new ShardRouter({ shardCount: 2, virtualBuckets: 8 });
  // Rebalance: buckets 6 and 7 move to a new shard 2.
  const after = new ShardRouter({ shardCount: 3, virtualBuckets: 8, bucketMap: [0, 0, 0, 0, 1, 1, 2, 2] });
  for (let i = 0; i < 500; i += 1) {
    const key = `CUST-${i}`;
    const bucket = before.bucketFor(key);
    assert.equal(after.bucketFor(key), bucket, 'bucket of a key never changes');
    if (bucket < 6) assert.equal(after.shardFor(key), before.shardFor(key));
    else assert.equal(after.shardFor(key), 2);
  }
});

test('rejects invalid configuration and keys', () => {
  assert.throws(() => new ShardRouter({ shardCount: 0 }));
  assert.throws(() => new ShardRouter({ shardCount: 4, virtualBuckets: 2 }));
  assert.throws(() => new ShardRouter({ shardCount: 2, virtualBuckets: 4, bucketMap: [0, 1, 2, 0] }));
  const router = new ShardRouter({ shardCount: 2 });
  assert.throws(() => router.route(''), TypeError);
  assert.throws(() => router.route(undefined), TypeError);
});
