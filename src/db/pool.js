import pg from 'pg';

export function createPool(connectionString, { name, max = 10, statementTimeoutMs = 30_000, logger } = {}) {
  const pool = new pg.Pool({
    connectionString,
    max,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 5_000,
    statement_timeout: statementTimeoutMs,
    application_name: `orders-ingestion:${name}`,
  });
  // An idle client losing its connection must not crash the process; the next checkout reconnects.
  pool.on('error', (err) => logger?.error({ err, pool: name }, 'idle postgres client error'));
  return pool;
}

export async function withTransaction(pool, fn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // Connection is already broken; the original error is the useful one.
    }
    throw err;
  } finally {
    client.release();
  }
}

export async function ping(pool, timeoutMs = 2000) {
  const started = Date.now();
  await Promise.race([
    pool.query('SELECT 1'),
    new Promise((_, reject) => setTimeout(() => reject(new Error(`ping timed out after ${timeoutMs}ms`)), timeoutMs).unref()),
  ]);
  return Date.now() - started;
}
