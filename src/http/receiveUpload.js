import { createHash, randomUUID } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { rm } from 'node:fs/promises';
import path from 'node:path';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import busboy from 'busboy';
import { HttpError } from '../utils/errors.js';

/**
 * Streams the single multipart file field to a temp file while computing its SHA-256 and size.
 * The upload is never buffered in memory; memory use is constant regardless of file size.
 *
 * @returns {Promise<{ path: string, originalName: string, mimeType: string, sizeBytes: number, sha256: string }>}
 */
export function receiveUpload(req, { fieldName = 'file', maxBytes, tmpDir }) {
  return new Promise((resolve, reject) => {
    if (!req.is('multipart/form-data')) {
      reject(new HttpError(415, 'UNSUPPORTED_MEDIA_TYPE', `expected multipart/form-data with a "${fieldName}" field`));
      return;
    }

    let parser;
    try {
      parser = busboy({ headers: req.headers, limits: { files: 1, fileSize: maxBytes, fields: 20 } });
    } catch (err) {
      reject(new HttpError(400, 'MALFORMED_MULTIPART', err.message));
      return;
    }

    let filePromise = null;
    let failed = false;

    parser.on('file', (name, stream, info) => {
      if (name !== fieldName || filePromise) {
        stream.resume(); // drain fields we don't want
        return;
      }
      const tmpPath = path.join(tmpDir, `orders-upload-${randomUUID()}`);
      const hash = createHash('sha256');
      let sizeBytes = 0;
      let truncated = false;
      stream.on('limit', () => {
        truncated = true;
      });
      const hasher = new Transform({
        transform(chunk, _encoding, callback) {
          hash.update(chunk);
          sizeBytes += chunk.length;
          callback(null, chunk);
        },
      });

      filePromise = pipeline(stream, hasher, createWriteStream(tmpPath))
        .then(() => {
          if (truncated) throw new HttpError(413, 'FILE_TOO_LARGE', `file exceeds the ${maxBytes} byte limit`);
          return { path: tmpPath, originalName: info.filename ?? 'upload.csv', mimeType: info.mimeType, sizeBytes, sha256: hash.digest('hex') };
        })
        .catch(async (err) => {
          await rm(tmpPath, { force: true });
          throw err;
        });
      // Avoid an unhandled rejection if the request errors before 'close'; the error is surfaced below.
      filePromise.catch(() => {});
    });

    parser.on('close', async () => {
      if (failed) return;
      if (!filePromise) {
        reject(new HttpError(400, 'MISSING_FILE', `no file found in multipart field "${fieldName}"`));
        return;
      }
      try {
        resolve(await filePromise);
      } catch (err) {
        reject(err);
      }
    });

    parser.on('error', (err) => {
      failed = true;
      filePromise?.then((file) => rm(file.path, { force: true }), () => {});
      reject(new HttpError(400, 'MALFORMED_MULTIPART', err.message));
    });

    req.pipe(parser);
  });
}
