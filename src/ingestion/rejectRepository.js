/** Rejected rows are kept in the meta database so they can be inspected and fixed later. */
export class RejectRepository {
  constructor({ pool }) {
    this.pool = pool;
  }

  /** Batch insert; ON CONFLICT makes re-running a job idempotent (one row per job + line). */
  async insertMany(jobId, rejects) {
    if (rejects.length === 0) return 0;
    const { rowCount } = await this.pool.query(
      `INSERT INTO ingestion_errors (job_id, line_number, stage, errors, raw_record)
       SELECT $1::uuid, r.line_number, r.stage, ARRAY(SELECT jsonb_array_elements_text(r.errors)), r.raw_record
       FROM jsonb_to_recordset($2::jsonb) AS r(line_number int, stage text, errors jsonb, raw_record jsonb)
       ON CONFLICT (job_id, line_number) DO NOTHING`,
      [
        jobId,
        JSON.stringify(
          rejects.map((r) => ({ line_number: r.lineNumber, stage: r.stage, errors: r.errors, raw_record: r.raw ?? null })),
        ),
      ],
    );
    return rowCount;
  }

  async listForJob(jobId, { limit = 100, offset = 0 } = {}) {
    const { rows } = await this.pool.query(
      `SELECT line_number, stage, errors, raw_record, created_at
       FROM ingestion_errors WHERE job_id = $1
       ORDER BY line_number LIMIT $2 OFFSET $3`,
      [jobId, limit, offset],
    );
    return rows.map((row) => ({
      lineNumber: row.line_number,
      stage: row.stage,
      errors: row.errors,
      raw: row.raw_record,
      createdAt: row.created_at,
    }));
  }
}
