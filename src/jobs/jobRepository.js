const JOB_COLUMNS = `id, file_name, file_sha256, file_size_bytes, gcs_bucket, gcs_object, status, attempts,
  max_attempts, next_run_at, locked_by, locked_at, batch_size, shard_count, stats, last_error,
  created_at, updated_at, started_at, finished_at`;

/** Postgres-backed job queue living in the meta database (SELECT ... FOR UPDATE SKIP LOCKED). */
export class JobRepository {
  constructor({ pool }) {
    this.pool = pool;
  }

  async findById(id) {
    const { rows } = await this.pool.query(`SELECT ${JOB_COLUMNS} FROM ingestion_jobs WHERE id = $1`, [id]);
    return rows[0] ?? null;
  }

  async findBySha256(sha256) {
    const { rows } = await this.pool.query(`SELECT ${JOB_COLUMNS} FROM ingestion_jobs WHERE file_sha256 = $1`, [sha256]);
    return rows[0] ?? null;
  }

  /**
   * Inserts a queued job. If another request created a job for the same file in the meantime,
   * returns that one with `created: false`.
   */
  async create({ fileName, sha256, sizeBytes, gcsBucket, gcsObject, maxAttempts, batchSize, shardCount }) {
    const { rows } = await this.pool.query(
      `INSERT INTO ingestion_jobs (file_name, file_sha256, file_size_bytes, gcs_bucket, gcs_object, max_attempts, batch_size, shard_count)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (file_sha256) DO NOTHING
       RETURNING ${JOB_COLUMNS}`,
      [fileName, sha256, sizeBytes, gcsBucket, gcsObject, maxAttempts, batchSize, shardCount],
    );
    if (rows[0]) return { job: rows[0], created: true };
    return { job: await this.findBySha256(sha256), created: false };
  }

  /** Gives a failed job a fresh set of attempts (used when the same file is uploaded again). */
  async requeueFailed(id) {
    const { rows } = await this.pool.query(
      `UPDATE ingestion_jobs
       SET status = 'queued', attempts = 0, next_run_at = now(), last_error = NULL,
           finished_at = NULL, locked_by = NULL, locked_at = NULL, updated_at = now()
       WHERE id = $1 AND status = 'failed'
       RETURNING ${JOB_COLUMNS}`,
      [id],
    );
    return rows[0] ?? null;
  }

  /**
   * Atomically claims the next runnable job. Also reclaims jobs stuck in 'processing' whose
   * lock is older than `staleLockSeconds` (their worker crashed). SKIP LOCKED lets many workers
   * poll concurrently without blocking each other or double-claiming.
   */
  async claimNext(workerId, staleLockSeconds) {
    const { rows } = await this.pool.query(
      `UPDATE ingestion_jobs AS j
       SET status = 'processing', attempts = j.attempts + 1, locked_by = $1, locked_at = now(),
           started_at = COALESCE(j.started_at, now()), updated_at = now()
       WHERE j.id = (
         SELECT id FROM ingestion_jobs
         WHERE (status = 'queued' AND next_run_at <= now())
            OR (status = 'processing' AND locked_at < now() - make_interval(secs => $2))
         ORDER BY next_run_at
         FOR UPDATE SKIP LOCKED
         LIMIT 1
       )
       RETURNING ${JOB_COLUMNS}`,
      [workerId, staleLockSeconds],
    );
    return rows[0] ?? null;
  }

  /** Saves progress and refreshes the lock (heartbeat). Returns false if the lock was lost. */
  async heartbeat(id, workerId, stats) {
    const { rowCount } = await this.pool.query(
      `UPDATE ingestion_jobs SET stats = $3, locked_at = now(), updated_at = now()
       WHERE id = $1 AND locked_by = $2 AND status = 'processing'`,
      [id, workerId, JSON.stringify(stats)],
    );
    return rowCount === 1;
  }

  async markCompleted(id, workerId, stats) {
    const { rows } = await this.pool.query(
      `UPDATE ingestion_jobs
       SET status = 'completed', stats = $3, last_error = NULL, locked_by = NULL, locked_at = NULL,
           finished_at = now(), updated_at = now()
       WHERE id = $1 AND locked_by = $2
       RETURNING ${JOB_COLUMNS}`,
      [id, workerId, JSON.stringify(stats)],
    );
    return rows[0] ?? null;
  }

  /** Re-queues with backoff while attempts remain and the error is retryable; otherwise fails the job. */
  async markFailed(id, workerId, { error, retryable, backoffSeconds, stats }) {
    const { rows } = await this.pool.query(
      `UPDATE ingestion_jobs
       SET status      = CASE WHEN $3 AND attempts < max_attempts THEN 'queued' ELSE 'failed' END,
           next_run_at = now() + make_interval(secs => $4),
           finished_at = CASE WHEN $3 AND attempts < max_attempts THEN NULL ELSE now() END,
           last_error  = $5, stats = $6, locked_by = NULL, locked_at = NULL, updated_at = now()
       WHERE id = $1 AND locked_by = $2
       RETURNING ${JOB_COLUMNS}`,
      [id, workerId, retryable, backoffSeconds, error, JSON.stringify(stats ?? {})],
    );
    return rows[0] ?? null;
  }

  async listRecent(limit = 20) {
    const { rows } = await this.pool.query(`SELECT ${JOB_COLUMNS} FROM ingestion_jobs ORDER BY created_at DESC LIMIT $1`, [limit]);
    return rows;
  }
}
