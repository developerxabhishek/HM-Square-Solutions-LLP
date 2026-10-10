import { rm } from 'node:fs/promises';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { setTimeout as sleep } from 'node:timers/promises';
import { Router } from 'express';
import { HttpError } from '../../utils/errors.js';
import { metrics } from '../../metrics.js';
import { presentJob } from '../presenters.js';
import { receiveUpload } from '../receiveUpload.js';

const CSV_MIME_TYPES = new Set(['text/csv', 'application/csv', 'text/plain', 'application/vnd.ms-excel']);

function isCsvUpload(file) {
  const extension = path.extname(file.originalName).toLowerCase();
  return extension === '.csv' || (extension === '' && CSV_MIME_TYPES.has(file.mimeType));
}

function isSettled(job) {
  return job.status === 'completed' || job.status === 'failed' || (job.status === 'queued' && job.attempts > 0);
}

async function waitForJob(jobs, jobId, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let job = await jobs.findById(jobId);
  while (!isSettled(job) && Date.now() < deadline) {
    await sleep(250);
    job = await jobs.findById(jobId);
  }
  return job;
}

/**
 * POST /upload-orders  (multipart/form-data, field "file")
 *
 * 1. stream the file to a temp file, hashing it (constant memory)
 * 2. same bytes already ingested?  -> 200 with the existing job (idempotent)
 * 3. upload to GCS as <prefix>/<sha256>.csv (ADC auth)
 * 4. enqueue an ingestion job                    -> 202 Accepted + job status URL
 *    (?wait=true blocks until the job settles     -> 200 with final stats)
 */
export function uploadRoutes({ storage, jobs, worker, router: shardRouter, config }) {
  const router = Router();

  router.post('/upload-orders', async (req, res) => {
    const file = await receiveUpload(req, { maxBytes: config.ingestion.maxUploadBytes, tmpDir: config.ingestion.tmpDir });
    let outcome;
    try {
      outcome = await storeAndEnqueue(req.log, file);
    } finally {
      // The spooled copy is no longer needed: the durable copy is in object storage.
      await rm(file.path, { force: true });
    }

    let { job } = outcome;
    if (outcome.duplicate) {
      res.status(200).json({ duplicate: true, ...presentJob(job, storage) });
      return;
    }
    if (req.query.wait === 'true') job = await waitForJob(jobs, job.id, config.ingestion.waitTimeoutMs);
    const done = job.status === 'completed' || job.status === 'failed';
    res
      .status(done ? 200 : 202)
      .location(`/jobs/${job.id}`)
      .json({ duplicate: false, ...presentJob(job, storage) });
  });

  async function storeAndEnqueue(log, file) {
    if (!isCsvUpload(file)) {
      throw new HttpError(415, 'UNSUPPORTED_FILE_TYPE', `only CSV files are accepted (got "${file.originalName}")`);
    }
    if (file.sizeBytes === 0) throw new HttpError(400, 'EMPTY_FILE', 'uploaded file is empty');
    log.info({ file: file.originalName, bytes: file.sizeBytes, sha256: file.sha256 }, 'upload received');

    const existing = await jobs.findBySha256(file.sha256);
    if (existing && existing.status !== 'failed') {
      metrics.uploads.inc({ result: 'duplicate' });
      log.info({ jobId: existing.id, status: existing.status }, 'file already ingested; returning existing job');
      return { job: existing, duplicate: true };
    }

    const objectName = `${config.storage.prefix}/${file.sha256}.csv`;
    const uploadStarted = performance.now();
    log.info({ object: objectName }, 'storage upload started');
    let uploaded;
    try {
      uploaded = await storage.upload(file.path, {
        destination: objectName,
        contentType: 'text/csv',
        sizeBytes: file.sizeBytes,
        metadata: { originalName: file.originalName, sha256: file.sha256 },
      });
    } catch (err) {
      metrics.uploads.inc({ result: 'storage_error' });
      log.error({ err, object: objectName }, 'storage upload failed');
      throw new HttpError(502, 'STORAGE_UPLOAD_FAILED', `could not upload file to storage: ${err.message}`);
    }
    log.info({ ...uploaded, ms: Math.round(performance.now() - uploadStarted) }, 'storage upload finished');

    let job;
    if (existing) {
      job = (await jobs.requeueFailed(existing.id)) ?? (await jobs.findById(existing.id));
      log.info({ jobId: job.id }, 'previously failed job re-queued');
    } else {
      ({ job } = await jobs.create({
        fileName: file.originalName,
        sha256: file.sha256,
        sizeBytes: file.sizeBytes,
        gcsBucket: uploaded.bucket,
        gcsObject: uploaded.object,
        maxAttempts: config.worker.maxAttempts,
        batchSize: config.ingestion.batchSize,
        shardCount: shardRouter.shardCount,
      }));
      log.info({ jobId: job.id }, 'ingestion job queued');
    }
    worker?.wake();
    metrics.uploads.inc({ result: 'accepted' });
    return { job, duplicate: false };
  }

  return router;
}
