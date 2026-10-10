// Applies migrations/meta to the meta database and migrations/shard to every shard.
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadConfig } from '../src/config.js';
import { createLogger } from '../src/logger.js';
import { createPool } from '../src/db/pool.js';
import { migrate } from '../src/db/migrate.js';

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const config = loadConfig();
const logger = createLogger({ level: config.logLevel, pretty: config.logPretty, name: 'migrate' });

if (!config.db.metaUrl || config.db.shardUrls.length === 0) {
  logger.error('META_DATABASE_URL and SHARD_DATABASE_URLS must be set');
  process.exit(1);
}

const targets = [
  { name: 'meta', url: config.db.metaUrl, dir: path.join(root, 'migrations/meta') },
  ...config.db.shardUrls.map((url, i) => ({ name: `shard-${i}`, url, dir: path.join(root, 'migrations/shard') })),
];

let failed = false;
for (const target of targets) {
  const pool = createPool(target.url, { name: `migrate-${target.name}`, max: 1, logger });
  try {
    await migrate(pool, target.dir, { logger, target: target.name });
  } catch (err) {
    failed = true;
    logger.error({ err, target: target.name }, 'migration failed');
  } finally {
    await pool.end();
  }
}
process.exit(failed ? 1 : 0);
