import { createPool } from './db/pool.js';
import { ShardedDatabase } from './db/shardedDatabase.js';
import { ShardRouter } from './db/shardRouter.js';
import { RejectRepository } from './ingestion/rejectRepository.js';
import { createJobProcessor } from './jobs/createJobProcessor.js';
import { JobRepository } from './jobs/jobRepository.js';
import { JobWorker } from './jobs/jobWorker.js';
import { OrdersRepository } from './orders/ordersRepository.js';
import { createStorage } from './storage/index.js';

/** Composition root: wires every dependency once. */
export function createContainer(config, logger) {
  const { poolMax, statementTimeoutMs } = config.db;
  const router = new ShardRouter({ shardCount: config.db.shardUrls.length, virtualBuckets: config.sharding.virtualBuckets });
  const shardDb = new ShardedDatabase({ shardUrls: config.db.shardUrls, poolMax, statementTimeoutMs, logger });
  const metaPool = createPool(config.db.metaUrl, { name: 'meta', max: poolMax, statementTimeoutMs, logger });
  const storage = createStorage(config.storage, logger);

  const ordersRepo = new OrdersRepository({ db: shardDb, router });
  const jobs = new JobRepository({ pool: metaPool });
  const rejectRepo = new RejectRepository({ pool: metaPool });

  const workerId = config.instanceId;
  const processJob = createJobProcessor({ storage, router, ordersRepo, rejectRepo, jobs, config, logger, workerId });
  const worker = new JobWorker({
    jobs,
    processJob,
    logger,
    workerId,
    concurrency: config.worker.concurrency,
    pollIntervalMs: config.worker.pollIntervalMs,
    staleLockSeconds: config.worker.staleLockSeconds,
  });

  return {
    config,
    logger,
    router,
    shardDb,
    metaPool,
    storage,
    ordersRepo,
    jobs,
    rejectRepo,
    worker,
    /** Logs whether GCS credentials (ADC) resolve; never throws. */
    async checkStorage() {
      if (!storage.checkCredentials) return;
      const result = await storage.checkCredentials();
      if (result.ok) logger.info({ bucket: config.storage.bucket, credentials: result.type }, 'GCS credentials resolved via ADC');
      else if (storage.apiEndpoint) {
        logger.warn(
          'No Google Application Default Credentials found. Not needed for the GCS emulator; real GCS requires ' +
            '`gcloud auth application-default login`.',
        );
      } else {
        logger.error(
          { bucket: config.storage.bucket, error: result.error },
          'No Google Application Default Credentials found. Run `gcloud auth application-default login` ' +
            '(or attach a service account when deployed). Uploads to GCS will fail until then.',
        );
      }
    },
    async close() {
      await worker.stop();
      await Promise.allSettled([shardDb.close(), metaPool.end()]);
    },
  };
}
