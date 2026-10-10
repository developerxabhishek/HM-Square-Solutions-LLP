import { setTimeout as sleep } from 'node:timers/promises';

/**
 * Runs `fn` until it succeeds, `isRetryable(err)` returns false, or `retries` is exhausted.
 * Uses exponential backoff with full jitter.
 */
export async function withRetry(fn, { retries = 3, baseDelayMs = 200, maxDelayMs = 5000, isRetryable = () => true, onRetry } = {}) {
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await fn(attempt);
    } catch (err) {
      if (attempt >= retries || !isRetryable(err)) throw err;
      const ceiling = Math.min(maxDelayMs, baseDelayMs * 2 ** attempt);
      const delayMs = Math.round(Math.random() * ceiling);
      onRetry?.(err, { attempt: attempt + 1, delayMs });
      await sleep(delayMs);
    }
  }
}

/** Backoff for re-queued jobs, in seconds: 30s, 60s, 120s ... capped at 15 min. */
export function jobBackoffSeconds(attempts) {
  return Math.min(900, 30 * 2 ** Math.max(0, attempts - 1));
}
