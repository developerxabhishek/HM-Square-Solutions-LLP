import express from 'express';
import { pinoHttp } from 'pino-http';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { HttpError } from '../utils/errors.js';
import { jobRoutes, orderRoutes } from './routes/queryRoutes.js';
import { opsRoutes } from './routes/opsRoutes.js';
import { uploadRoutes } from './routes/uploadRoutes.js';

const PUBLIC_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../public');

/** Builds the Express app from injected dependencies (no globals, so tests can pass fakes). */
export function createApp(deps) {
  const { logger } = deps;
  const app = express();
  app.disable('x-powered-by');
  app.set('query parser', 'simple');

  app.use(
    pinoHttp({
      logger,
      // req.log carries only the request id; the full request is logged once on completion.
      quietReqLogger: true,
      genReqId: (req, res) => {
        const id = req.headers['x-request-id'] || randomUUID();
        res.setHeader('x-request-id', id);
        return id;
      },
      autoLogging: { ignore: (req) => req.url === '/health/live' || req.url === '/metrics' },
      customLogLevel: (_req, res, err) => (err || res.statusCode >= 500 ? 'error' : res.statusCode >= 400 ? 'warn' : 'info'),
    }),
  );

  // Small operator UI (public/index.html): upload, job progress, rejected rows, order lookup.
  app.use(express.static(PUBLIC_DIR, { index: 'index.html', maxAge: '5m' }));

  app.use(opsRoutes(deps));
  app.use(uploadRoutes(deps));
  app.use(orderRoutes(deps));
  app.use(jobRoutes(deps));

  app.use((req, _res, next) => next(new HttpError(404, 'NOT_FOUND', `no route for ${req.method} ${req.path}`)));

  // Express 5 forwards rejected promises from async handlers here.
  // eslint-disable-next-line no-unused-vars
  app.use((err, req, res, _next) => {
    if (err instanceof HttpError) {
      res.status(err.status).json({ error: { code: err.code, message: err.message, details: err.details } });
      return;
    }
    req.log.error({ err }, 'unhandled request error');
    res.status(500).json({ error: { code: 'INTERNAL_ERROR', message: 'internal server error', requestId: req.id } });
  });

  return app;
}
