// Standalone worker process: scale ingestion independently of the HTTP API.
import { assertConfig, loadConfig } from './config.js';
import { createContainer } from './container.js';
import { createLogger } from './logger.js';
import { installShutdownHandlers } from './shutdown.js';

const config = assertConfig(loadConfig());
const logger = createLogger({ level: config.logLevel, pretty: config.logPretty, name: 'orders-ingestion-worker' });
const container = createContainer(config, logger);

container.worker.start();
container.checkStorage();
installShutdownHandlers({ logger, steps: [() => container.close()] });
