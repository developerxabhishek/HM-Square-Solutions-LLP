import { after, beforeEach, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import request from 'supertest';
import { createApp } from '../src/http/app.js';
import { ShardRouter } from '../src/db/shardRouter.js';
import { silentLogger } from './helpers/fakes.js';

const tmpDir = await mkdtemp(path.join(os.tmpdir(), 'orders-http-test-'));
after(() => rm(tmpDir, { recursive: true, force: true }));

class FakeJobs {
  constructor() {
    this.jobs = new Map();
  }
  async findById(id) {
    return this.jobs.get(id) ?? null;
  }
  async findBySha256(sha) {
    return [...this.jobs.values()].find((j) => j.file_sha256 === sha) ?? null;
  }
  async create(input) {
    const job = {
      id: `00000000-0000-4000-8000-${String(this.jobs.size + 1).padStart(12, '0')}`,
      file_name: input.fileName,
      file_sha256: input.sha256,
      file_size_bytes: input.sizeBytes,
      gcs_bucket: input.gcsBucket,
      gcs_object: input.gcsObject,
      status: 'queued',
      attempts: 0,
      max_attempts: input.maxAttempts,
      stats: {},
    };
    this.jobs.set(job.id, job);
    return { job, created: true };
  }
  async listRecent(limit) {
    return [...this.jobs.values()].reverse().slice(0, limit);
  }
  async requeueFailed(id) {
    const job = this.jobs.get(id);
    Object.assign(job, { status: 'queued', attempts: 0 });
    return job;
  }
}

class FakeStorage {
  constructor() {
    this.bucketName = 'test-bucket';
    this.uploads = [];
    this.fail = false;
  }
  async upload(localPath, { destination }) {
    if (this.fail) throw new Error('403 permission denied');
    this.uploads.push(destination);
    return { bucket: this.bucketName, object: destination, alreadyExisted: false };
  }
  uri(object) {
    return `gs://${this.bucketName}/${object}`;
  }
}

let deps;
beforeEach(() => {
  deps = {
    logger: silentLogger,
    jobs: new FakeJobs(),
    storage: new FakeStorage(),
    worker: { woken: 0, wake() { this.woken += 1; } },
    router: new ShardRouter({ shardCount: 2 }),
    rejectRepo: { listForJob: async () => [] },
    ordersRepo: {
      findById: async (id) => (id === 'O1' ? { orderId: 'O1', shardId: 1 } : null),
      findByCustomer: async (customerId, opts) => ({ shardId: 0, orders: [{ orderId: 'O1', customerId }], nextCursor: null, opts }),
    },
    metaPool: { query: async () => ({ rows: [] }) },
    shardDb: { health: async () => [{ shardId: 0, ok: true }, { shardId: 1, ok: true }] },
    config: {
      storage: { prefix: 'orders/uploads' },
      ingestion: { maxUploadBytes: 1024, tmpDir, waitTimeoutMs: 1000, batchSize: 1000 },
      worker: { maxAttempts: 3 },
    },
  };
});

const CSV = 'order_id,customer_id,order_date,order_amount,status\nO1,C1,2024-01-01,1.00,pending\n';

test('POST /upload-orders stores the file and queues a job (202)', async () => {
  const res = await request(createApp(deps)).post('/upload-orders').attach('file', Buffer.from(CSV), 'orders.csv');
  assert.equal(res.status, 202);
  assert.equal(res.body.status, 'queued');
  assert.equal(res.body.duplicate, false);
  assert.match(res.body.file.storageUri, /^gs:\/\/test-bucket\/orders\/uploads\/[0-9a-f]{64}\.csv$/);
  assert.equal(res.headers.location, `/jobs/${res.body.jobId}`);
  assert.equal(deps.storage.uploads.length, 1);
  assert.equal(deps.worker.woken, 1);
  assert.deepEqual(await readdir(tmpDir), [], 'temp file cleaned up');
});

test('uploading identical content again returns the existing job without re-uploading', async () => {
  const app = createApp(deps);
  const first = await request(app).post('/upload-orders').attach('file', Buffer.from(CSV), 'a.csv');
  const second = await request(app).post('/upload-orders').attach('file', Buffer.from(CSV), 'b.csv');
  assert.equal(second.status, 200);
  assert.equal(second.body.duplicate, true);
  assert.equal(second.body.jobId, first.body.jobId);
  assert.equal(deps.storage.uploads.length, 1);
});

test('a re-upload of a failed file re-queues the same job', async () => {
  const app = createApp(deps);
  const first = await request(app).post('/upload-orders').attach('file', Buffer.from(CSV), 'a.csv');
  deps.jobs.jobs.get(first.body.jobId).status = 'failed';
  const second = await request(app).post('/upload-orders').attach('file', Buffer.from(CSV), 'a.csv');
  assert.equal(second.status, 202);
  assert.equal(second.body.jobId, first.body.jobId);
  assert.equal(second.body.status, 'queued');
});

test('upload validation errors', async () => {
  const app = createApp(deps);
  assert.equal((await request(app).post('/upload-orders').send({ a: 1 })).status, 415);
  assert.equal((await request(app).post('/upload-orders').field('other', 'x')).status, 400);
  assert.equal((await request(app).post('/upload-orders').attach('file', Buffer.from('x'), 'orders.xlsx')).status, 415);
  assert.equal((await request(app).post('/upload-orders').attach('file', Buffer.alloc(0), 'orders.csv')).status, 400);
  const tooBig = await request(app).post('/upload-orders').attach('file', Buffer.alloc(2048, 'a'), 'orders.csv');
  assert.equal(tooBig.status, 413);
  assert.equal(tooBig.body.error.code, 'FILE_TOO_LARGE');
  assert.deepEqual(await readdir(tmpDir), []);
});

test('storage failures surface as 502 and no job is created', async () => {
  deps.storage.fail = true;
  const res = await request(createApp(deps)).post('/upload-orders').attach('file', Buffer.from(CSV), 'orders.csv');
  assert.equal(res.status, 502);
  assert.equal(res.body.error.code, 'STORAGE_UPLOAD_FAILED');
  assert.equal(deps.jobs.jobs.size, 0);
});

test('GET /orders endpoints', async () => {
  const app = createApp(deps);
  assert.equal((await request(app).get('/orders/O1')).body.shardId, 1);
  assert.equal((await request(app).get('/orders/NOPE')).status, 404);
  assert.equal((await request(app).get('/orders')).status, 400);
  assert.equal((await request(app).get('/orders?customerId=C1&limit=0')).status, 400);
  assert.equal((await request(app).get('/orders?customerId=C1&cursor=garbage')).status, 400);
  const page = await request(app).get('/orders?customerId=C1&limit=10');
  assert.equal(page.status, 200);
  assert.equal(page.body.orders[0].customerId, 'C1');
});

test('GET /jobs/:id validates the id and 404s unknown jobs', async () => {
  const app = createApp(deps);
  assert.equal((await request(app).get('/jobs/not-a-uuid')).status, 400);
  assert.equal((await request(app).get('/jobs/00000000-0000-4000-8000-000000000999')).status, 404);
});

test('GET /jobs lists recent uploads, newest first', async () => {
  const app = createApp(deps);
  await request(app).post('/upload-orders').attach('file', Buffer.from(CSV), 'first.csv');
  await request(app).post('/upload-orders').attach('file', Buffer.from(`${CSV}O2,C2,2024-01-02,2.00,pending\n`), 'second.csv');
  const res = await request(app).get('/jobs?limit=5');
  assert.equal(res.status, 200);
  assert.deepEqual(res.body.jobs.map((j) => j.file.name), ['second.csv', 'first.csv']);
  assert.equal((await request(app).get('/jobs?limit=500')).status, 400);
});

test('GET / serves the UI', async () => {
  const res = await request(createApp(deps)).get('/');
  assert.equal(res.status, 200);
  assert.match(res.headers['content-type'], /text\/html/);
  assert.match(res.text, /<title>Orders Ingestion<\/title>/);
});

test('health and metrics endpoints', async () => {
  const app = createApp(deps);
  const health = await request(app).get('/health');
  assert.equal(health.status, 200);
  assert.equal(health.body.checks.shards.length, 2);
  deps.shardDb.health = async () => [{ shardId: 0, ok: false, error: 'down' }];
  assert.equal((await request(createApp(deps)).get('/health')).status, 503);
  const metricsRes = await request(app).get('/metrics');
  assert.match(metricsRes.text, /orders_uploads_total/);
});
