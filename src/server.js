import { assertConfig, loadConfig } from './config.js';
import { createContainer } from './container.js';
import { createApp } from './http/app.js';
import { createLogger } from './logger.js';
import { installShutdownHandlers } from './shutdown.js';

const config = assertConfig(loadConfig());
const logger = createLogger({ level: config.logLevel, pretty: config.logPretty });
const container = createContainer(config, logger);

// Workers can also run as separate processes (src/worker.js); the embedded one is convenient
// for single-node deployments and local development.
if (config.worker.embedded) container.worker.start();

const app = createApp({ ...container, worker: config.worker.embedded ? container.worker : null });
const server = app.listen(config.port, () => {
  logger.info(
    { port: config.port, shards: config.db.shardUrls.length, storage: config.storage.driver, embeddedWorker: config.worker.embedded },
    'orders ingestion API listening',
  );
});
server.requestTimeout = 15 * 60 * 1000;
container.checkStorage();

installShutdownHandlers({
  logger,
  steps: [() => new Promise((resolve) => server.close(resolve)), () => container.close()],
});
