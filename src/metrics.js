import client from 'prom-client';

export const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry, prefix: 'orders_ingestion_' });

export const metrics = {
  uploads: new client.Counter({
    name: 'orders_uploads_total',
    help: 'Upload requests by result',
    labelNames: ['result'],
    registers: [registry],
  }),
  rows: new client.Counter({
    name: 'orders_rows_total',
    help: 'Processed order rows by outcome',
    labelNames: ['outcome'],
    registers: [registry],
  }),
  jobs: new client.Counter({
    name: 'orders_jobs_finished_total',
    help: 'Ingestion job attempts by final status',
    labelNames: ['status'],
    registers: [registry],
  }),
  batchWriteSeconds: new client.Histogram({
    name: 'orders_batch_write_seconds',
    help: 'Latency of one batch transaction per shard',
    labelNames: ['shard'],
    buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [registry],
  }),
  jobSeconds: new client.Histogram({
    name: 'orders_job_duration_seconds',
    help: 'Duration of ingestion job attempts',
    buckets: [0.5, 1, 2, 5, 10, 30, 60, 120, 300],
    registers: [registry],
  }),
};
