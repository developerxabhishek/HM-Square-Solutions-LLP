import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createReadStream } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { ShardRouter } from '../src/db/shardRouter.js';
import { processOrderFile } from '../src/ingestion/processOrderFile.js';
import { PermanentJobError } from '../src/utils/errors.js';
import { csvStream, FakeOrdersRepo, FakeRejectRepo, silentLogger } from './helpers/fakes.js';

const HEADER = 'order_id,customer_id,order_date,order_amout,status';

function run(text, { batchSize = 3, shardCount = 3, ordersRepo = new FakeOrdersRepo(), rejectRepo = new FakeRejectRepo(), onProgress } = {}) {
  const router = new ShardRouter({ shardCount });
  const promise = processOrderFile({
    job: { id: '00000000-0000-4000-8000-000000000001' },
    source: csvStream(text),
    router,
    ordersRepo,
    rejectRepo,
    logger: silentLogger,
    batchSize,
    rejectBatchSize: 2,
    progressEveryRows: 5,
    onProgress,
  });
  return { promise, router, ordersRepo, rejectRepo };
}

function rows(n, { customers = 7 } = {}) {
  return Array.from({ length: n }, (_, i) => `ORD-${i},CUST-${i % customers},2024-01-${String((i % 28) + 1).padStart(2, '0')}T00:00:00Z,${i}.50,pending`);
}

test('routes every valid row to its shard in batches no larger than batchSize', async () => {
  const { promise, router, ordersRepo } = run([HEADER, ...rows(50)].join('\n'));
  const stats = await promise;

  assert.equal(stats.totalRows, 50);
  assert.equal(stats.validRows, 50);
  assert.equal(stats.inserted, 50);
  assert.equal(stats.rejectedRows, 0);

  const written = ordersRepo.batches.flatMap((b) => b.orders.map((o) => ({ ...o, shardId: b.shardId })));
  assert.equal(written.length, 50);
  for (const order of written) assert.equal(order.shardId, router.shardFor(order.customerId), `${order.orderId} on wrong shard`);
  for (const batch of ordersRepo.batches) assert.ok(batch.orders.length <= 3);
  // batch numbers are a gap-free sequence per shard (the ledger relies on that)
  for (const shardId of new Set(ordersRepo.batches.map((b) => b.shardId))) {
    const numbers = ordersRepo.batches.filter((b) => b.shardId === shardId).map((b) => b.batchNo).sort((a, b) => a - b);
    assert.deepEqual(numbers, numbers.map((_, i) => i));
  }
});

test('invalid and malformed rows are rejected with line numbers; valid rows still load', async () => {
  const text = [
    HEADER,
    'O1,C1,2024-01-01T00:00:00Z,10.00,pending', // line 2 ok
    'O2,C1,not-a-date,10.00,pending', // line 3 validation
    'O3,C2,2024-01-01T00:00:00Z,10.00', // line 4 too few fields
    '', // empty line skipped
    'O4,C3,2024-01-01T00:00:00Z,"5.00",shipped,extra', // line 6 too many fields
    'O5,C3,2024-01-01T00:00:00Z,"12.00"x,shipped', // line 7 broken quoting
    'O6,C4,2024-01-02T00:00:00Z,7.25,delivered', // line 8 ok
  ].join('\n');
  const { promise, rejectRepo, ordersRepo } = run(text);
  const stats = await promise;

  assert.equal(stats.totalRows, 6);
  assert.equal(stats.validRows, 2);
  assert.equal(stats.rejectedRows, 4);
  assert.deepEqual(stats.rejectedByStage, { parse: 3, validation: 1, database: 0 });
  assert.deepEqual(rejectRepo.rows.map((r) => r.lineNumber).sort((a, b) => a - b), [3, 4, 6, 7]);
  assert.deepEqual(rejectRepo.rows.find((r) => r.lineNumber === 3).raw.order_date, 'not-a-date');
  assert.deepEqual(ordersRepo.batches.flatMap((b) => b.orders.map((o) => o.orderId)).sort(), ['O1', 'O6']);
});

test('duplicate order_ids in a batch: last occurrence wins', async () => {
  const text = [HEADER, 'O1,C1,2024-01-01,1.00,pending', 'O1,C1,2024-01-01,2.00,shipped'].join('\n');
  const { promise, ordersRepo } = run(text, { batchSize: 10 });
  const stats = await promise;
  assert.equal(stats.duplicateRows, 1);
  const orders = ordersRepo.batches.flatMap((b) => b.orders);
  assert.equal(orders.length, 1);
  assert.equal(orders[0].orderAmount, '2.00');
});

test('rows rejected by the database are recorded as rejects', async () => {
  const ordersRepo = new FakeOrdersRepo({ rejectOrderIds: ['ORD-3'] });
  const { promise, rejectRepo } = run([HEADER, ...rows(10)].join('\n'), { ordersRepo });
  const stats = await promise;
  assert.equal(stats.dbRejected, 1);
  assert.equal(stats.rejectedByStage.database, 1);
  assert.equal(rejectRepo.rows.find((r) => r.stage === 'database').raw.orderId, 'ORD-3');
});

test('transient database errors are retried', async () => {
  const ordersRepo = new FakeOrdersRepo({ failTimes: 2, failWith: Object.assign(new Error('conn reset'), { code: 'ECONNRESET' }) });
  const stats = await run([HEADER, ...rows(4)].join('\n'), { ordersRepo, batchSize: 100 }).promise;
  assert.equal(stats.inserted, 4);
});

test('non-transient database errors fail the run', async () => {
  const ordersRepo = new FakeOrdersRepo({ failTimes: 1, failWith: Object.assign(new Error('relation "orders" does not exist'), { code: '42P01' }) });
  await assert.rejects(run([HEADER, ...rows(4)].join('\n'), { ordersRepo, batchSize: 100 }).promise, /does not exist/);
});

test('re-running the same job replays committed batches instead of rewriting them', async () => {
  const ordersRepo = new FakeOrdersRepo();
  const text = [HEADER, ...rows(20)].join('\n');
  await run(text, { ordersRepo }).promise;
  const batchesAfterFirstRun = ordersRepo.batches.length;
  const second = await run(text, { ordersRepo }).promise;
  assert.equal(ordersRepo.batches.length, batchesAfterFirstRun);
  assert.equal(second.batchesReplayed, second.batches);
});

test('reports progress periodically', async () => {
  const seen = [];
  await run([HEADER, ...rows(12)].join('\n'), { onProgress: async (s) => seen.push(s.totalRows) }).promise;
  assert.deepEqual(seen, [5, 10]);
});

test('missing columns or an empty file are permanent errors', async () => {
  await assert.rejects(run('order_id,customer_id\nO1,C1\n').promise, PermanentJobError);
  await assert.rejects(run('').promise, PermanentJobError);
});

test('a permanent error is not masked when the source is a file stream', async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'orders-src-'));
  const file = path.join(dir, 'bad.csv');
  await writeFile(file, 'id,customer,amount\n1,2,3\n');
  try {
    const promise = processOrderFile({
      job: { id: '00000000-0000-4000-8000-000000000002' },
      source: createReadStream(file),
      router: new ShardRouter({ shardCount: 2 }),
      ordersRepo: new FakeOrdersRepo(),
      rejectRepo: new FakeRejectRepo(),
      logger: silentLogger,
      batchSize: 10,
    });
    await assert.rejects(promise, PermanentJobError);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('handles BOM, CRLF line endings and quoted fields', async () => {
  const text = `﻿${HEADER}\r\n"O,1","C 1","2024-01-01T00:00:00Z","10.00","pending"\r\n`;
  const { promise, ordersRepo } = run(text);
  const stats = await promise;
  assert.equal(stats.validRows, 1);
  assert.equal(ordersRepo.batches[0].orders[0].orderId, 'O,1');
});
