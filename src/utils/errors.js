/** Error that maps directly to an HTTP response. */
export class HttpError extends Error {
  constructor(status, code, message, details) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

/**
 * Failure that will not go away by retrying (bad header, empty file, changed topology).
 * The worker marks the job failed immediately instead of re-queueing it.
 */
export class PermanentJobError extends Error {
  constructor(message) {
    super(message);
    this.name = 'PermanentJobError';
  }
}

// SQLSTATE classes/codes worth retrying: connection problems, serialization/deadlock, resource limits, shutdown.
const RETRYABLE_SQLSTATE_PREFIXES = ['08', '53', '57P', '58'];
const RETRYABLE_SQLSTATES = new Set(['40001', '40P01', '55P03']);
const RETRYABLE_NODE_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN']);

export function isTransientDbError(err) {
  if (!err) return false;
  const code = err.code;
  if (typeof code !== 'string') return /Connection terminated|timeout exceeded when trying to connect/i.test(err.message ?? '');
  if (RETRYABLE_NODE_CODES.has(code) || RETRYABLE_SQLSTATES.has(code)) return true;
  return RETRYABLE_SQLSTATE_PREFIXES.some((prefix) => code.startsWith(prefix));
}

/** Class 22 (data exception) and 23 (integrity violation): the row is bad, not the database. */
export function isRowDataError(err) {
  return typeof err?.code === 'string' && /^(22|23)/.test(err.code);
}
