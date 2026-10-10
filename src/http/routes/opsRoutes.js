import { Router } from 'express';
import { ping } from '../../db/pool.js';
import { registry } from '../../metrics.js';

export function opsRoutes({ metaPool, shardDb, router: shardRouter, storage }) {
  const router = Router();

  // Liveness: the process is up and serving HTTP.
  router.get('/health/live', (_req, res) => {
    res.json({ status: 'ok' });
  });

  // Readiness: every database this instance depends on answers.
  router.get('/health', async (_req, res) => {
    const [meta, shards] = await Promise.all([
      ping(metaPool).then(
        (latencyMs) => ({ ok: true, latencyMs }),
        (err) => ({ ok: false, error: err.message }),
      ),
      shardDb.health(),
    ]);
    const healthy = meta.ok && shards.every((shard) => shard.ok);
    res.status(healthy ? 200 : 503).json({
      status: healthy ? 'ok' : 'degraded',
      uptimeSeconds: Math.round(process.uptime()),
      checks: { metaDatabase: meta, shards },
      storage: {
        driver: storage.constructor.name,
        bucket: storage.bucketName,
        endpoint: storage.apiEndpoint ?? (storage.constructor.name === 'GcsStorage' ? 'https://storage.googleapis.com' : null),
        credentials: storage.credentials,
      },
    });
  });

  // Shard topology, handy when reasoning about where data lives.
  router.get('/shards', (_req, res) => {
    res.json(shardRouter.describe());
  });

  // Prometheus scrape endpoint.
  router.get('/metrics', async (_req, res) => {
    res.set('Content-Type', registry.contentType).send(await registry.metrics());
  });

  return router;
}
