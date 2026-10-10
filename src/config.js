import os from 'node:os';
import dotenv from 'dotenv';

dotenv.config({ quiet: true });

function str(name, fallback) {
  const value = process.env[name];
  return value === undefined || value === '' ? fallback : value.trim();
}

function int(name, fallback) {
  const raw = str(name);
  if (raw === undefined) return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isFinite(value)) throw new Error(`Environment variable ${name} must be an integer, got "${raw}"`);
  return value;
}

function bool(name, fallback) {
  const raw = str(name);
  if (raw === undefined) return fallback;
  return ['1', 'true', 'yes', 'on'].includes(raw.toLowerCase());
}

function list(name) {
  return (str(name, '') || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

export function loadConfig() {
  return Object.freeze({
    env: str('NODE_ENV', 'development'),
    port: int('PORT', 3000),
    logLevel: str('LOG_LEVEL', 'info'),
    logPretty: bool('LOG_PRETTY', false),
    instanceId: str('INSTANCE_ID', `${os.hostname()}-${process.pid}`),

    storage: Object.freeze({
      // "gcs" is the real implementation. "local" exists only for offline tests/demos.
      driver: str('STORAGE_DRIVER', 'gcs'),
      bucket: str('GCS_BUCKET'),
      projectId: str('GOOGLE_CLOUD_PROJECT'),
      prefix: str('GCS_UPLOAD_PREFIX', 'orders/uploads'),
      // Only for local GCS emulators (fake-gcs-server). Leave unset to use real GCS.
      apiEndpoint: str('GCS_API_ENDPOINT'),
      localDir: str('LOCAL_STORAGE_DIR', './.local-storage'),
    }),

    db: Object.freeze({
      metaUrl: str('META_DATABASE_URL'),
      // Ordered list: index N in this list IS shard N. Never reorder in an existing deployment.
      shardUrls: list('SHARD_DATABASE_URLS'),
      poolMax: int('PG_POOL_MAX', 10),
      statementTimeoutMs: int('PG_STATEMENT_TIMEOUT_MS', 30_000),
    }),

    sharding: Object.freeze({
      // Fixed number of logical buckets. Changing it re-maps every key, so it is set once per cluster.
      virtualBuckets: int('SHARD_VIRTUAL_BUCKETS', 1024),
    }),

    ingestion: Object.freeze({
      batchSize: int('INSERT_BATCH_SIZE', 1000),
      rejectBatchSize: int('REJECT_BATCH_SIZE', 500),
      maxUploadBytes: int('MAX_UPLOAD_BYTES', 100 * 1024 * 1024),
      tmpDir: str('UPLOAD_TMP_DIR', os.tmpdir()),
      progressEveryRows: int('PROGRESS_EVERY_ROWS', 2000),
      waitTimeoutMs: int('UPLOAD_WAIT_TIMEOUT_MS', 60_000),
    }),

    worker: Object.freeze({
      embedded: bool('RUN_EMBEDDED_WORKER', true),
      concurrency: int('WORKER_CONCURRENCY', 2),
      pollIntervalMs: int('WORKER_POLL_INTERVAL_MS', 1000),
      maxAttempts: int('JOB_MAX_ATTEMPTS', 3),
      staleLockSeconds: int('JOB_STALE_LOCK_SECONDS', 300),
    }),
  });
}

export function assertConfig(config) {
  const problems = [];
  if (!config.db.metaUrl) problems.push('META_DATABASE_URL is required');
  if (config.db.shardUrls.length === 0) problems.push('SHARD_DATABASE_URLS must list at least one shard');
  if (!['gcs', 'local'].includes(config.storage.driver)) problems.push('STORAGE_DRIVER must be "gcs" or "local"');
  if (config.storage.driver === 'gcs' && !config.storage.bucket) problems.push('GCS_BUCKET is required');
  if (config.ingestion.batchSize < 1 || config.ingestion.batchSize > 10_000) {
    problems.push('INSERT_BATCH_SIZE must be between 1 and 10000');
  }
  if (config.sharding.virtualBuckets < config.db.shardUrls.length || config.sharding.virtualBuckets > 32_767) {
    problems.push('SHARD_VIRTUAL_BUCKETS must be >= shard count and <= 32767');
  }
  if (problems.length > 0) {
    throw new Error(`Invalid configuration:\n  - ${problems.join('\n  - ')}`);
  }
  return config;
}
