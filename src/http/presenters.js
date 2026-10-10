export function presentJob(job, storage) {
  return {
    jobId: job.id,
    status: job.status,
    attempts: job.attempts,
    maxAttempts: job.max_attempts,
    file: {
      name: job.file_name,
      sizeBytes: Number(job.file_size_bytes),
      sha256: job.file_sha256,
      storageUri: storage?.uri ? storage.uri(job.gcs_object) : `gs://${job.gcs_bucket}/${job.gcs_object}`,
    },
    stats: job.stats,
    lastError: job.last_error,
    nextRunAt: job.status === 'queued' ? job.next_run_at : undefined,
    createdAt: job.created_at,
    startedAt: job.started_at,
    finishedAt: job.finished_at,
    links: { self: `/jobs/${job.id}`, errors: `/jobs/${job.id}/errors` },
  };
}
