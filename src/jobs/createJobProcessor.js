import { processOrderFile } from '../ingestion/processOrderFile.js';
import { PermanentJobError } from '../utils/errors.js';

/** Builds the function the worker runs for each claimed job: GCS object -> sharded Postgres. */
export function createJobProcessor({ storage, router, ordersRepo, rejectRepo, jobs, config, logger, workerId }) {
  return async function processJob(job) {
    // Batch boundaries (and so the per-shard ledger) depend on shard count and batch size,
    // which are frozen on the job. A different topology needs an explicit reshard, not a retry.
    if (job.shard_count !== router.shardCount) {
      throw new PermanentJobError(`job was created for ${job.shard_count} shards but ${router.shardCount} are configured`);
    }

    return processOrderFile({
      job,
      source: storage.createReadStream(job.gcs_object),
      router,
      ordersRepo,
      rejectRepo,
      logger: logger.child({ jobId: job.id }),
      batchSize: job.batch_size,
      rejectBatchSize: config.ingestion.rejectBatchSize,
      progressEveryRows: config.ingestion.progressEveryRows,
      onProgress: async (stats) => {
        const stillOwned = await jobs.heartbeat(job.id, workerId, stats);
        if (!stillOwned) throw new Error('job lock lost (another worker reclaimed it); aborting this attempt');
      },
    });
  };
}
