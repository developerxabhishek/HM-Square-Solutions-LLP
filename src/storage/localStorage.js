import { createReadStream } from 'node:fs';
import { copyFile, mkdir, stat } from 'node:fs/promises';
import path from 'node:path';

/**
 * Filesystem stand-in for GCS with the same interface. Only for running tests and the app
 * offline (STORAGE_DRIVER=local); production uses GcsStorage.
 */
export class LocalStorage {
  constructor({ dir, bucket = 'local' }) {
    this.root = path.resolve(dir);
    this.bucketName = bucket;
  }

  #resolve(objectName) {
    const target = path.resolve(this.root, objectName);
    if (!target.startsWith(this.root + path.sep)) throw new Error(`invalid object name: ${objectName}`);
    return target;
  }

  async upload(localPath, { destination }) {
    const target = this.#resolve(destination);
    const exists = await stat(target).then(() => true, () => false);
    if (!exists) {
      await mkdir(path.dirname(target), { recursive: true });
      await copyFile(localPath, target);
    }
    return { bucket: this.bucketName, object: destination, alreadyExisted: exists };
  }

  createReadStream(objectName) {
    return createReadStream(this.#resolve(objectName));
  }

  uri(objectName) {
    return `file://${this.#resolve(objectName)}`;
  }
}
