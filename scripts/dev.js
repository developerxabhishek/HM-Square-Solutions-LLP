// One-command local environment: `npm run dev`
//
//   1. starts a local PostgreSQL (embedded-postgres, no install needed), data kept in .dev-postgres/
//   2. creates the meta database + 3 shard databases (if missing)
//   3. applies migrations
//   4. generates sample-data/orders.csv (if missing)
//   5. starts storage (see below)
//   6. starts the API + worker + web UI with auto-restart on code changes
//
// Storage, in order of preference:
//   - real Google Cloud Storage, if GCS_BUCKET is set in .env (auth via ADC)
//   - otherwise a local GCS emulator (fake-gcs-server, downloaded once into .dev-tools/). The app
//     runs the exact same GcsStorage code and Google client library; only the endpoint differs.
//   - `--local` (or if the emulator can't run): plain local folder
//
// Ctrl+C stops everything. `npm run dev -- --reset` wipes local databases and stored files first.
import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync } from 'node:fs';
import { chmod, mkdir, mkdtemp, rename, rm, writeFile } from 'node:fs/promises';
import net from 'node:net';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import EmbeddedPostgres from 'embedded-postgres';
import pg from 'pg';
import { loadConfig } from '../src/config.js';
import { migrate } from '../src/db/migrate.js';

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const PG_DIR = path.join(root, '.dev-postgres');
const PG_PORT = Number(process.env.DEV_PG_PORT ?? 54329);
const PG_USER = 'postgres';
const PG_PASSWORD = 'postgres';
const DATABASES = ['orders_meta', 'orders_shard_0', 'orders_shard_1', 'orders_shard_2'];
const SAMPLE_FILE = path.join(root, 'sample-data', 'orders.csv');

const EMULATOR_VERSION = '1.56.1';
const EMULATOR_PORT = Number(process.env.DEV_GCS_PORT ?? 4443);
const EMULATOR_ENDPOINT = `http://127.0.0.1:${EMULATOR_PORT}`;
const EMULATOR_BUCKET = 'orders-dev';
const TOOLS_DIR = path.join(root, '.dev-tools');
const EMULATOR_BIN = path.join(TOOLS_DIR, `fake-gcs-server-${EMULATOR_VERSION}`);
const EMULATOR_DATA_DIR = path.join(root, '.dev-gcs');

const log = (msg) => console.log(`\x1b[36m[dev]\x1b[0m ${msg}`);
const dbUrl = (db) => `postgres://${PG_USER}:${PG_PASSWORD}@localhost:${PG_PORT}/${db}`;

const config = loadConfig(); // reads .env
const realGcs =
  config.storage.driver === 'gcs' && !config.storage.apiEndpoint && config.storage.bucket && config.storage.bucket !== 'your-orders-bucket';
let storageMode = process.argv.includes('--local') ? 'local' : realGcs ? 'gcs' : 'emulator';

/** True if something accepts TCP connections on the port (i.e. it is taken). */
function portInUse(port) {
  return new Promise((resolve) => {
    const socket = net.connect({ port, host: '127.0.0.1' });
    socket.once('connect', () => {
      socket.destroy();
      resolve(true);
    });
    socket.once('error', () => resolve(false));
  });
}

// Refuse to start (and above all to --reset) while another instance is running: deleting
// .dev-postgres under a live PostgreSQL corrupts it.
const busy = [];
if (await portInUse(PG_PORT)) busy.push(`PostgreSQL port ${PG_PORT}`);
if (await portInUse(config.port)) busy.push(`app port ${config.port}`);
if (busy.length > 0) {
  console.error(
    `\n${busy.join(' and ')} already in use. Is \`npm run dev\` already running (maybe in another terminal)?\n` +
      'Stop it first (Ctrl+C in its terminal, or: pkill -f scripts/dev.js), then run this again.\n',
  );
  process.exit(1);
}

if (process.argv.includes('--reset')) {
  log('--reset: deleting local databases (.dev-postgres) and stored files (.local-storage, .dev-gcs)');
  await rm(PG_DIR, { recursive: true, force: true });
  await rm(path.join(root, '.local-storage'), { recursive: true, force: true });
  await rm(EMULATOR_DATA_DIR, { recursive: true, force: true });
}

async function download(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`GET ${url} -> HTTP ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}

/** Downloads the pinned fake-gcs-server release once, verifying its SHA-256 against the release checksums. */
async function ensureEmulatorBinary() {
  if (existsSync(EMULATOR_BIN)) return EMULATOR_BIN;
  const os = { linux: 'Linux', darwin: 'Darwin' }[process.platform];
  const arch = { x64: 'amd64', arm64: 'arm64' }[process.arch];
  if (!os || !arch) throw new Error(`no fake-gcs-server build for ${process.platform}/${process.arch}`);
  const asset = `fake-gcs-server_${EMULATOR_VERSION}_${os}_${arch}.tar.gz`;
  const base = `https://github.com/fsouza/fake-gcs-server/releases/download/v${EMULATOR_VERSION}`;
  log(`downloading GCS emulator fake-gcs-server v${EMULATOR_VERSION} into .dev-tools/ (first run only)`);
  const [archive, checksums] = await Promise.all([download(`${base}/${asset}`), download(`${base}/checksums.txt`)]);
  const expected = checksums.toString().split('\n').find((line) => line.trim().endsWith(asset))?.split(/\s+/)[0];
  const actual = createHash('sha256').update(archive).digest('hex');
  if (!expected || expected !== actual) throw new Error(`checksum mismatch for ${asset}`);

  await mkdir(TOOLS_DIR, { recursive: true });
  const extractDir = await mkdtemp(path.join(TOOLS_DIR, 'extract-'));
  try {
    const archivePath = path.join(extractDir, asset);
    await writeFile(archivePath, archive);
    const untar = spawnSync('tar', ['-xzf', archivePath, '-C', extractDir, 'fake-gcs-server'], { encoding: 'utf8' });
    if (untar.status !== 0) throw new Error(`tar failed: ${untar.stderr}`);
    await chmod(path.join(extractDir, 'fake-gcs-server'), 0o755);
    await rename(path.join(extractDir, 'fake-gcs-server'), EMULATOR_BIN);
  } finally {
    await rm(extractDir, { recursive: true, force: true });
  }
  return EMULATOR_BIN;
}

/** Starts fake-gcs-server (data persisted in .dev-gcs/) and makes sure the bucket exists. */
async function startEmulator() {
  if (await fetch(`${EMULATOR_ENDPOINT}/storage/v1/b`).then(() => true, () => false)) {
    throw new Error(`port ${EMULATOR_PORT} is already in use (set DEV_GCS_PORT to change it)`);
  }
  const bin = await ensureEmulatorBinary();
  const child = spawn(
    bin,
    ['-scheme', 'http', '-host', '127.0.0.1', '-port', String(EMULATOR_PORT), '-backend', 'filesystem',
      '-filesystem-root', EMULATOR_DATA_DIR, '-public-host', `127.0.0.1:${EMULATOR_PORT}`, '-log-level', 'warn'],
    { stdio: ['ignore', 'inherit', 'inherit'] },
  );
  let exited = false;
  child.on('exit', () => {
    exited = true;
  });
  for (let i = 0; i < 40; i += 1) {
    if (exited) throw new Error('fake-gcs-server exited during startup');
    if (await fetch(`${EMULATOR_ENDPOINT}/storage/v1/b`).then((r) => r.ok, () => false)) break;
    await sleep(150);
  }
  const bucket = await fetch(`${EMULATOR_ENDPOINT}/storage/v1/b/${EMULATOR_BUCKET}`);
  if (bucket.status === 404) {
    const created = await fetch(`${EMULATOR_ENDPOINT}/storage/v1/b?project=dev`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: EMULATOR_BUCKET }),
    });
    if (!created.ok) throw new Error(`could not create emulator bucket: HTTP ${created.status}`);
    log(`created bucket gs://${EMULATOR_BUCKET} in the emulator`);
  }
  return child;
}

// 1. PostgreSQL ---------------------------------------------------------------
const postgres = new EmbeddedPostgres({
  databaseDir: PG_DIR,
  port: PG_PORT,
  user: PG_USER,
  password: PG_PASSWORD,
  persistent: true,
  onLog: () => {},
  onError: (err) => console.error('[postgres]', err),
});

if (!existsSync(path.join(PG_DIR, 'PG_VERSION'))) {
  log('initialising a local PostgreSQL cluster in .dev-postgres/ (first run only)');
  await postgres.initialise();
}
log(`starting PostgreSQL on port ${PG_PORT}`);
try {
  await postgres.start();
} catch {
  console.error(
    `\nPostgreSQL could not start. Most likely port ${PG_PORT} is already in use (another \`npm run dev\` still running?).\n` +
      'Stop the other instance, or pick another port: DEV_PG_PORT=54330 npm run dev\n',
  );
  process.exit(1);
}

// 2. Databases ------------------------------------------------------------------
const admin = new pg.Client(dbUrl('postgres'));
await admin.connect();
const { rows } = await admin.query('SELECT datname FROM pg_database WHERE datname = ANY($1)', [DATABASES]);
const existing = new Set(rows.map((row) => row.datname));
for (const db of DATABASES.filter((name) => !existing.has(name))) {
  await admin.query(`CREATE DATABASE ${admin.escapeIdentifier(db)}`);
  log(`created database ${db}`);
}
await admin.end();

// 3. Migrations -----------------------------------------------------------------
const targets = [
  { name: 'meta', db: DATABASES[0], dir: 'migrations/meta' },
  ...DATABASES.slice(1).map((db, i) => ({ name: `shard-${i}`, db, dir: 'migrations/shard' })),
];
for (const target of targets) {
  const pool = new pg.Pool({ connectionString: dbUrl(target.db), max: 1 });
  try {
    const applied = await migrate(pool, path.join(root, target.dir), { target: target.name });
    if (applied.length) log(`${target.name}: applied ${applied.join(', ')}`);
  } finally {
    await pool.end();
  }
}
log('schema up to date on meta + 3 shards');

// 4. Sample data ----------------------------------------------------------------
if (!existsSync(SAMPLE_FILE)) {
  spawnSync(process.execPath, [path.join(root, 'scripts/generate-orders.js'), '--out', SAMPLE_FILE], { stdio: 'inherit' });
}

// 5. Storage ----------------------------------------------------------------------
let emulator = null;
if (storageMode === 'emulator') {
  try {
    emulator = await startEmulator();
    log(`GCS emulator running at ${EMULATOR_ENDPOINT}`);
  } catch (err) {
    log(`GCS emulator unavailable (${err.message}); falling back to a local folder`);
    storageMode = 'local';
  }
}
const storageEnv = {
  gcs: {},
  emulator: { STORAGE_DRIVER: 'gcs', GCS_BUCKET: EMULATOR_BUCKET, GCS_API_ENDPOINT: EMULATOR_ENDPOINT },
  local: { STORAGE_DRIVER: 'local' },
}[storageMode];
const storageLabel = {
  gcs: `Google Cloud Storage: gs://${config.storage.bucket} (auth: Application Default Credentials)`,
  emulator: `GCS emulator at ${EMULATOR_ENDPOINT}, bucket gs://${EMULATOR_BUCKET}\n` +
    '                  (same GcsStorage code + Google client library as production; set GCS_BUCKET in .env for real GCS)',
  local: 'local folder .local-storage/ (offline mode)',
}[storageMode];

// 6. App ------------------------------------------------------------------------
const env = {
  ...process.env,
  META_DATABASE_URL: dbUrl(DATABASES[0]),
  SHARD_DATABASE_URLS: DATABASES.slice(1).map(dbUrl).join(','),
  LOG_PRETTY: process.env.LOG_PRETTY ?? 'true',
  RUN_EMBEDDED_WORKER: 'true',
  ...storageEnv,
};
const app = spawn(process.execPath, ['--watch', '--watch-preserve-output', 'src/server.js'], { cwd: root, env, stdio: 'inherit' });

const baseUrl = `http://localhost:${config.port}`;
let up = false;
for (let i = 0; i < 60 && !up; i += 1) {
  up = await fetch(`${baseUrl}/health/live`).then((r) => r.ok, () => false);
  if (!up) await sleep(250);
}
if (!up) {
  log(`the app is not answering on ${baseUrl}; check the errors above (port ${config.port} in use? set PORT in .env)`);
} else console.log(`
\x1b[32m  Everything is running.\x1b[0m

    Web UI        ${baseUrl}
    API health    ${baseUrl}/health
    Sample file   ${path.relative(process.cwd(), SAMPLE_FILE)}  (upload it from the UI)
    Storage       ${storageLabel}
    PostgreSQL    localhost:${PG_PORT}  user=${PG_USER} password=${PG_PASSWORD}
                  databases: ${DATABASES.join(', ')}

    Code changes in src/ restart the app automatically. Press Ctrl+C to stop everything.
`);

// Shutdown --------------------------------------------------------------------
// Ctrl+C reaches the app directly (same process group); embedded-postgres stops PostgreSQL
// on exit via its own exit hook. This handles `kill <pid>` of this script as well.
let stopping = false;
const stop = async () => {
  if (stopping) return;
  stopping = true;
  app.kill('SIGTERM');
  emulator?.kill('SIGTERM');
  await postgres.stop();
  process.exit(0);
};
process.on('SIGTERM', stop);
process.on('SIGINT', () => {
  // embedded-postgres' exit hook stops PostgreSQL; make sure the app and emulator go too even
  // when SIGINT was sent to this process only (not via Ctrl+C to the whole terminal group).
  stopping = true;
  app.kill('SIGINT');
  emulator?.kill('SIGINT');
});
app.on('exit', async (code, signal) => {
  if (stopping || signal === 'SIGINT') return;
  log(`app exited (code ${code}); stopping PostgreSQL`);
  emulator?.kill('SIGTERM');
  await postgres.stop();
  process.exit(code ?? 1);
});
