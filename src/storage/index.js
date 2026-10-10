import { GcsStorage } from './gcsStorage.js';
import { LocalStorage } from './localStorage.js';

export function createStorage(storageConfig, logger) {
  if (storageConfig.driver === 'local') {
    logger?.warn({ dir: storageConfig.localDir }, 'STORAGE_DRIVER=local: files are NOT uploaded to GCS (offline/testing mode)');
    return new LocalStorage({ dir: storageConfig.localDir });
  }
  if (storageConfig.apiEndpoint) {
    logger?.warn({ apiEndpoint: storageConfig.apiEndpoint }, 'GCS_API_ENDPOINT set: using a GCS emulator, not storage.googleapis.com');
  }
  return new GcsStorage({
    bucket: storageConfig.bucket,
    projectId: storageConfig.projectId,
    apiEndpoint: storageConfig.apiEndpoint,
    logger,
  });
}
