import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';

// Arbitrary constant; serialises concurrent migrators (e.g. several containers starting at once).
const MIGRATION_LOCK_KEY = 7_241_901;

/** Applies every *.sql file in `dir` not yet recorded in schema_migrations, in filename order. */
export async function migrate(pool, dir, { logger, target = 'db' } = {}) {
  const files = (await readdir(dir)).filter((file) => file.endsWith('.sql')).sort();
  const client = await pool.connect();
  try {
    await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_KEY]);
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version    TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )`);
    const { rows } = await client.query('SELECT version FROM schema_migrations');
    const applied = new Set(rows.map((row) => row.version));

    const pending = files.filter((file) => !applied.has(file));
    for (const file of pending) {
      const sql = await readFile(path.join(dir, file), 'utf8');
      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations (version) VALUES ($1)', [file]);
        await client.query('COMMIT');
        logger?.info({ target, migration: file }, 'migration applied');
      } catch (err) {
        await client.query('ROLLBACK');
        throw new Error(`migration ${file} failed on ${target}: ${err.message}`, { cause: err });
      }
    }
    if (pending.length === 0) logger?.info({ target }, 'schema up to date');
    return pending;
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_KEY]).catch(() => {});
    client.release();
  }
}
