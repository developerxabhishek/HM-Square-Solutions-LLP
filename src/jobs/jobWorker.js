import { performance } from 'node:perf_hooks';
import { PermanentJobError } from '../utils/errors.js';
import { jobBackoffSeconds } from '../utils/retry.js';
import { metrics } from '../metrics.js';

/**
 * Polls the job queue and runs jobs with bounded concurrency. Any number of these can run
 * (inside API processes and/or as standalone `npm run start:worker` processes); the queue's
 * SKIP LOCKED claim guarantees each job attempt is executed by one worker.
 */
export class JobWorker {
  constructor({ jobs, processJob, logger, workerId, concurrency = 1, pollIntervalMs = 1000, staleLockSeconds = 300 }) {
    this.jobs = jobs;
    this.processJob = processJob;
    this.logger = logger.child({ component: 'worker', workerId });
    this.workerId = workerId;
    this.concurrency = concurrency;
    this.pollIntervalMs = pollIntervalMs;
    this.staleLockSeconds = staleLockSeconds;
    this.running = false;
    this.loops = [];
    this.sleepers = new Set();
  }

  start() {
    if (this.running) return;
    this.running = true;
    this.loops = Array.from({ length: this.concurrency }, (_, slot) => this.#loop(slot));
    this.logger.info({ concurrency: this.concurrency }, 'worker started');
  }

  /** Lets idle slots poll immediately (called right after a job is enqueued). */
  wake() {
    for (const wakeUp of this.sleepers) wakeUp();
  }

  /** Stops claiming new jobs and waits for in-flight jobs to finish. */
  async stop() {
    this.running = false;
    this.wake();
    await Promise.allSettled(this.loops);
    this.logger.info('worker stopped');
  }

  async #loop(slot) {
    while (this.running) {
      let job = null;
      try {
        job = await this.jobs.claimNext(this.workerId, this.staleLockSeconds);
      } catch (err) {
        this.logger.error({ err, slot }, 'failed to poll job queue');
      }
      if (job) await this.runJob(job);
      else await this.#idle();
    }
  }

  #idle() {
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(timer);
        this.sleepers.delete(done);
        resolve();
      };
      const timer = setTimeout(done, this.pollIntervalMs);
      this.sleepers.add(done);
    });
  }

  async runJob(job) {
    const log = this.logger.child({ jobId: job.id, attempt: job.attempts });
    const started = performance.now();
    log.info({ file: job.file_name, object: job.gcs_object }, 'processing started');
    try {
      const stats = await this.processJob(job);
      await this.jobs.markCompleted(job.id, this.workerId, stats);
      metrics.jobs.inc({ status: 'completed' });
      log.info({ stats }, 'processing completed');
    } catch (err) {
      const retryable = !(err instanceof PermanentJobError);
      const updated = await this.jobs
        .markFailed(job.id, this.workerId, {
          error: err.message,
          retryable,
          backoffSeconds: jobBackoffSeconds(job.attempts),
        })
        .catch((markErr) => {
          log.error({ err: markErr }, 'could not record job failure; stale-lock recovery will retry it');
          return null;
        });
      const status = updated?.status ?? 'unknown';
      metrics.jobs.inc({ status: status === 'queued' ? 'retry_scheduled' : 'failed' });
      log.error({ err, retryable, status, nextRunAt: updated?.next_run_at }, 'processing failed');
    } finally {
      metrics.jobSeconds.observe((performance.now() - started) / 1000);
    }
  }
}
