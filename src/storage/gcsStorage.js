import { IdempotencyStrategy, Storage } from '@google-cloud/storage';

/**
 * Google Cloud Storage adapter.
 *
 * Authentication: `new Storage()` is created WITHOUT credentials, so the client library resolves
 * Application Default Credentials (ADC) in this order:
 *   1. GOOGLE_APPLICATION_CREDENTIALS (if someone sets it; we never do)
 *   2. ~/.config/gcloud/application_default_credentials.json  (`gcloud auth application-default login`)
 *   3. the attached service account / Workload Identity via the metadata server (GKE, Cloud Run, GCE)
 * No key file or secret is referenced by this code or the repository.
 */
export class GcsStorage {
  /**
   * @param {object} opts
   * @param {string} [opts.apiEndpoint] only for a local GCS emulator (e.g. fake-gcs-server);
   *   unset in real deployments, where the client talks to storage.googleapis.com.
   */
  constructor({ bucket, projectId, apiEndpoint, logger }) {
    this.bucketName = bucket;
    this.apiEndpoint = apiEndpoint ?? null;
    this.logger = logger;
    this.client = new Storage({
      ...(projectId ? { projectId } : {}),
      ...(apiEndpoint ? { apiEndpoint } : {}),
      retryOptions: {
        autoRetry: true,
        maxRetries: 5,
        retryDelayMultiplier: 2,
        totalTimeout: 600,
        // Uploads below carry an ifGenerationMatch precondition, which makes them safe to retry.
        idempotencyStrategy: IdempotencyStrategy.RetryConditional,
      },
    });
    this.bucket = this.client.bucket(bucket);
    this.credentials = { ok: null }; // filled in by checkCredentials()
  }

  /** Resolves ADC once so a missing login is reported at startup instead of on the first upload. */
  async checkCredentials() {
    try {
      const authClient = await this.client.authClient.getClient();
      this.credentials = { ok: true, type: authClient.constructor.name };
    } catch (err) {
      this.credentials = { ok: false, error: err.message.split('\n')[0] };
    }
    return this.credentials;
  }

  /**
   * Uploads a local file. Objects are content-addressed (named by SHA-256), so if the object
   * already exists (HTTP 412 on the `ifGenerationMatch: 0` precondition) it holds the same
   * bytes and the upload is treated as done.
   */
  async upload(localPath, { destination, contentType, metadata, sizeBytes }) {
    try {
      await this.bucket.upload(localPath, {
        destination,
        contentType,
        resumable: sizeBytes > 8 * 1024 * 1024,
        validation: 'crc32c',
        preconditionOpts: { ifGenerationMatch: 0 },
        metadata: { metadata },
      });
      return { bucket: this.bucketName, object: destination, alreadyExisted: false };
    } catch (err) {
      if (err?.code === 412) {
        this.logger?.info({ object: destination }, 'object already in bucket, skipping upload');
        return { bucket: this.bucketName, object: destination, alreadyExisted: true };
      }
      throw err;
    }
  }

  /** Streams an object back; workers read from GCS so they don't need the API host's disk. */
  createReadStream(objectName) {
    return this.bucket.file(objectName).createReadStream({ validation: 'crc32c' });
  }

  uri(objectName) {
    return `gs://${this.bucketName}/${objectName}`;
  }
}
