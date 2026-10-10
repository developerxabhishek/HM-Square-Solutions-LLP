import { pipeline } from 'node:stream/promises';
import { performance } from 'node:perf_hooks';
import { PermanentJobError } from '../utils/errors.js';
import { metrics } from '../metrics.js';
import { LineSplitter, parseCsvLine } from './csvLines.js';
import { resolveHeader, validateOrder } from './orderValidator.js';
import { RejectSink } from './rejectSink.js';
import { ShardedBatchWriter } from './shardedBatchWriter.js';

const MAX_LINE_LENGTH = 64 * 1024;
const MAX_RAW_STORED = 2048;

/**
 * Streams a CSV order file end to end:
 *
 *   source -> LineSplitter -> parseCsvLine -> validateOrder -> ShardedBatchWriter -> shard N
 *                                  \______________\______________ RejectSink -> ingestion_errors
 *
 * Only the current line plus the per-shard batch buffers are in memory at any time, and every
 * `await` in the consumer loop propagates backpressure up to the source stream.
 *
 * @param {object} p
 * @param {{ id: string }} p.job
 * @param {import('node:stream').Readable} p.source
 * @param {(stats: object) => Promise<unknown>} [p.onProgress] called every `progressEveryRows` rows
 */
export async function processOrderFile({
  job,
  source,
  router,
  ordersRepo,
  rejectRepo,
  logger,
  batchSize,
  rejectBatchSize = 500,
  progressEveryRows = 2000,
  onProgress,
}) {
  const started = performance.now();
  const rejects = new RejectSink({ repo: rejectRepo, jobId: job.id, batchSize: rejectBatchSize, logger });
  const writer = new ShardedBatchWriter({
    router,
    repo: ordersRepo,
    jobId: job.id,
    batchSize,
    logger,
    onRejected: (reject) => rejects.add(reject),
  });

  let totalRows = 0;
  let validRows = 0;
  const snapshot = () => ({
    totalRows,
    validRows,
    rejectedRows: rejects.count,
    rejectedByStage: { ...rejects.byStage },
    ...writer.stats,
    durationMs: Math.round(performance.now() - started),
  });

  let header = null;

  const handleLine = async ({ lineNumber, text, tooLong }) => {
    if (!tooLong && text.trim() === '') return; // blank lines are not rows

    if (!header) {
      const parsed = tooLong ? { error: 'header line too long' } : parseCsvLine(text);
      if (parsed.error) throw new PermanentJobError(`unreadable header: ${parsed.error}`);
      header = resolveHeader(parsed.fields);
      logger?.info({ jobId: job.id, columns: header.columns }, 'header accepted');
      return;
    }

    totalRows += 1;
    if (tooLong) {
      await rejects.add({ lineNumber, stage: 'parse', errors: [`line exceeds ${MAX_LINE_LENGTH} characters`], raw: null });
      return;
    }
    const parsed = parseCsvLine(text);
    if (parsed.error) {
      await rejects.add({ lineNumber, stage: 'parse', errors: [parsed.error], raw: { line: text.slice(0, MAX_RAW_STORED) } });
      return;
    }
    if (parsed.fields.length !== header.columnCount) {
      await rejects.add({
        lineNumber,
        stage: 'parse',
        errors: [`expected ${header.columnCount} fields, found ${parsed.fields.length}`],
        raw: header.toObject(parsed.fields),
      });
      return;
    }
    const result = validateOrder(header.pick(parsed.fields));
    if (!result.ok) {
      await rejects.add({ lineNumber, stage: 'validation', errors: result.errors, raw: header.toObject(parsed.fields) });
      return;
    }
    validRows += 1;
    await writer.add({ ...result.order, lineNumber });
  };

  let lastReportedRows = 0;
  let consumerError;
  try {
    await pipeline(source, new LineSplitter({ maxLineLength: MAX_LINE_LENGTH }), async (lines) => {
      try {
        for await (const line of lines) {
          await handleLine(line);
          if (onProgress && totalRows - lastReportedRows >= progressEveryRows) {
            lastReportedRows = totalRows;
            await onProgress(snapshot());
            logger?.info({ jobId: job.id, rows: totalRows }, 'processing progress');
          }
        }
      } catch (err) {
        consumerError = err;
        throw err;
      }
    });
  } catch (err) {
    // With some sources (e.g. fs streams) pipeline() reports a generic AbortError when the
    // consumer throws; surface the consumer's own error so PermanentJobError stays permanent.
    throw consumerError ?? err;
  }

  if (!header) throw new PermanentJobError('file is empty (no header row)');

  await writer.flushAll();
  await rejects.flush();

  const stats = snapshot();
  metrics.rows.inc({ outcome: 'inserted' }, stats.inserted);
  metrics.rows.inc({ outcome: 'updated' }, stats.updated);
  metrics.rows.inc({ outcome: 'unchanged' }, stats.unchanged);
  metrics.rows.inc({ outcome: 'rejected' }, stats.rejectedRows);
  return stats;
}
