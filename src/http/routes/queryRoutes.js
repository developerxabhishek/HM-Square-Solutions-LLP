import { Router } from 'express';
import { HttpError } from '../../utils/errors.js';
import { presentJob } from '../presenters.js';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function parseLimit(raw, { fallback, max }) {
  if (raw === undefined) return fallback;
  const limit = Number(raw);
  if (!Number.isInteger(limit) || limit < 1 || limit > max) {
    throw new HttpError(400, 'INVALID_LIMIT', `limit must be an integer between 1 and ${max}`);
  }
  return limit;
}

function decodeCursor(raw) {
  if (!raw) return undefined;
  try {
    const cursor = JSON.parse(Buffer.from(raw, 'base64url').toString('utf8'));
    if (typeof cursor.orderDate !== 'string' || typeof cursor.orderId !== 'string') throw new Error('bad shape');
    return cursor;
  } catch {
    throw new HttpError(400, 'INVALID_CURSOR', 'cursor is malformed');
  }
}

const encodeCursor = (cursor) => (cursor ? Buffer.from(JSON.stringify(cursor)).toString('base64url') : null);

export function orderRoutes({ ordersRepo }) {
  const router = Router();

  // GET /orders?customerId=C123&limit=50&cursor=...  -> single-shard query
  router.get('/orders', async (req, res) => {
    const customerId = typeof req.query.customerId === 'string' ? req.query.customerId.trim() : '';
    if (!customerId) throw new HttpError(400, 'MISSING_CUSTOMER_ID', 'query parameter customerId is required');
    const limit = parseLimit(req.query.limit, { fallback: 50, max: 500 });
    const page = await ordersRepo.findByCustomer(customerId, { limit, cursor: decodeCursor(req.query.cursor) });
    res.json({ customerId, shardId: page.shardId, count: page.orders.length, orders: page.orders, nextCursor: encodeCursor(page.nextCursor) });
  });

  // GET /orders/:orderId -> parallel fan-out to all shards
  router.get('/orders/:orderId', async (req, res) => {
    const order = await ordersRepo.findById(req.params.orderId);
    if (!order) throw new HttpError(404, 'ORDER_NOT_FOUND', `order ${req.params.orderId} not found`);
    res.json(order);
  });

  return router;
}

export function jobRoutes({ jobs, rejectRepo, storage }) {
  const router = Router();

  const loadJob = async (id) => {
    if (!UUID_RE.test(id)) throw new HttpError(400, 'INVALID_JOB_ID', 'job id must be a UUID');
    const job = await jobs.findById(id);
    if (!job) throw new HttpError(404, 'JOB_NOT_FOUND', `job ${id} not found`);
    return job;
  };

  router.get('/jobs', async (req, res) => {
    const limit = parseLimit(req.query.limit, { fallback: 20, max: 100 });
    const recent = await jobs.listRecent(limit);
    res.json({ jobs: recent.map((job) => presentJob(job, storage)) });
  });

  router.get('/jobs/:jobId', async (req, res) => {
    res.json(presentJob(await loadJob(req.params.jobId), storage));
  });

  router.get('/jobs/:jobId/errors', async (req, res) => {
    const job = await loadJob(req.params.jobId);
    const limit = parseLimit(req.query.limit, { fallback: 100, max: 1000 });
    const offset = Number(req.query.offset ?? 0);
    if (!Number.isInteger(offset) || offset < 0) throw new HttpError(400, 'INVALID_OFFSET', 'offset must be a non-negative integer');
    const errors = await rejectRepo.listForJob(job.id, { limit, offset });
    res.json({ jobId: job.id, totalRejected: job.stats?.rejectedRows ?? null, limit, offset, errors });
  });

  return router;
}
