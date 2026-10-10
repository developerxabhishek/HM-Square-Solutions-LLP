import { Readable } from 'node:stream';
import pino from 'pino';

export const silentLogger = pino({ level: 'silent' });

export class FakeOrdersRepo {
  constructor({ failTimes = 0, failWith, rejectOrderIds = [] } = {}) {
    this.batches = [];
    this.failTimes = failTimes;
    this.failWith = failWith;
    this.rejectOrderIds = new Set(rejectOrderIds);
    this.committed = new Set();
  }

  async writeBatch(shardId, orders, { jobId, batchNo }) {
    if (this.failTimes > 0) {
      this.failTimes -= 1;
      throw this.failWith;
    }
    const key = `${shardId}:${jobId}:${batchNo}`;
    if (this.committed.has(key)) return { inserted: orders.length, updated: 0, unchanged: 0, rejected: 0, rejectedRows: [], alreadyCommitted: true };
    this.committed.add(key);
    this.batches.push({ shardId, batchNo, orders });
    const rejectedRows = orders.filter((o) => this.rejectOrderIds.has(o.orderId)).map((order) => ({ order, error: '22003: numeric field overflow' }));
    return { inserted: orders.length - rejectedRows.length, updated: 0, unchanged: 0, rejected: rejectedRows.length, rejectedRows, alreadyCommitted: false };
  }
}

export class FakeRejectRepo {
  constructor() {
    this.rows = [];
  }

  async insertMany(jobId, rejects) {
    this.rows.push(...rejects.map((r) => ({ jobId, ...r })));
    return rejects.length;
  }

  async listForJob(jobId) {
    return this.rows.filter((r) => r.jobId === jobId);
  }
}

export function csvStream(text, chunkSize = 64) {
  // Small chunks make sure records spanning chunk boundaries are handled.
  const chunks = [];
  for (let i = 0; i < text.length; i += chunkSize) chunks.push(text.slice(i, i + chunkSize));
  return Readable.from(chunks);
}
