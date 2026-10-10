const LOGGED_REJECTS_LIMIT = 100;

/** Buffers rejected rows and persists them in batches; logs the first N individually. */
export class RejectSink {
  constructor({ repo, jobId, batchSize = 500, logger }) {
    this.repo = repo;
    this.jobId = jobId;
    this.batchSize = batchSize;
    this.logger = logger;
    this.buffer = [];
    this.count = 0;
    this.byStage = { parse: 0, validation: 0, database: 0 };
  }

  /** @param {{ lineNumber: number, stage: 'parse'|'validation'|'database', errors: string[], raw?: unknown }} reject */
  async add(reject) {
    this.count += 1;
    this.byStage[reject.stage] += 1;
    const level = this.count <= LOGGED_REJECTS_LIMIT ? 'warn' : 'debug';
    this.logger?.[level]({ jobId: this.jobId, line: reject.lineNumber, stage: reject.stage, errors: reject.errors }, 'order row rejected');
    if (this.count === LOGGED_REJECTS_LIMIT) {
      this.logger?.warn({ jobId: this.jobId }, `further rejected rows are logged at debug level; see GET /jobs/${this.jobId}/errors`);
    }
    this.buffer.push(reject);
    if (this.buffer.length >= this.batchSize) await this.flush();
  }

  async flush() {
    if (this.buffer.length === 0) return;
    const rejects = this.buffer;
    this.buffer = [];
    await this.repo.insertMany(this.jobId, rejects);
  }
}
