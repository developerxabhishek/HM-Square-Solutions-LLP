import pino from 'pino';

export function createLogger({ level = 'info', pretty = false, name = 'orders-ingestion' } = {}) {
  return pino({
    name,
    level,
    base: { pid: process.pid },
    timestamp: pino.stdTimeFunctions.isoTime,
    redact: { paths: ['req.headers.authorization', 'req.headers.cookie'], remove: true },
    ...(pretty ? { transport: { target: 'pino-pretty', options: { colorize: true } } } : {}),
  });
}
