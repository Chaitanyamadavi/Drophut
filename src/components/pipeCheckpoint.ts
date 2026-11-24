export type FileMetadata = {
  transferId: string;
  name: string;
  mimeType: string;
  size: number;
  totalChunks: number;
  chunkSize: number;
};

export type Checkpoint = FileMetadata & {
  nextChunk: number;
  state: "receiving" | "completed" | "cancelled";
};

const RETENTION_MS = 24 * 60 * 60 * 1000;
type StoredCheckpoint = Checkpoint & { key: string; updated: number };
let database: Promise<IDBDatabase> | undefined;

function open() {
  database ??= new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open("drophut-transfers", 1);
    request.onupgradeneeded = () => {
      request.result.createObjectStore("checkpoints", { keyPath: "key" });
      request.result.createObjectStore("batches");
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  }).catch((error) => {
    database = undefined;
    throw error;
  });
  return database;
}

function batchRange(key: string) {
  return IDBKeyRange.bound([key, 0], [key, Number.MAX_SAFE_INTEGER]);
}

function result<T>(request: IDBRequest<T>) {
  return new Promise<T>((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function committed(transaction: IDBTransaction) {
  return new Promise<void>((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onabort = () => reject(transaction.error ?? new Error("Local storage transaction aborted."));
  });
}

export async function loadCheckpoint(key: string): Promise<Checkpoint | undefined> {
  const db = await open();
  return result(db.transaction("checkpoints").objectStore("checkpoints").get(key));
}

export async function saveCheckpoint(key: string, checkpoint: Checkpoint, batch?: Blob) {
  const db = await open();
  const tx = db.transaction(["checkpoints", "batches"], "readwrite", { durability: "strict" });
  const done = committed(tx);
  const batches = tx.objectStore("batches");
  if (batch) batches.put(batch, [key, checkpoint.nextChunk]);
  if (checkpoint.state !== "receiving") batches.delete(batchRange(key));
  const record: StoredCheckpoint = { ...checkpoint, key, updated: Date.now() };
  tx.objectStore("checkpoints").put(record);
  await done;
}

export async function assembleFile(key: string, mimeType: string) {
  const db = await open();
  const batches: Blob[] = await result(db.transaction("batches").objectStore("batches").getAll(batchRange(key)));
  return new Blob(batches, { type: mimeType });
}

export async function deleteSessionBatches(prefix: string) {
  const db = await open();
  const tx = db.transaction(["checkpoints", "batches"], "readwrite");
  const done = committed(tx);
  const checkpoints = tx.objectStore("checkpoints");
  const request = checkpoints.openCursor();
  request.onsuccess = () => {
    const cursor = request.result;
    if (!cursor) return;
    const { key, updated } = cursor.value as StoredCheckpoint;
    if ((prefix && key.startsWith(prefix + ":")) || updated < Date.now() - RETENTION_MS) {
      tx.objectStore("batches").delete(batchRange(key));
      cursor.delete();
    }
    cursor.continue();
  };
  await done;
}

// Hash every byte in fixed blocks while keeping only one block and the running digest in memory.
export async function fingerprint(file: File, cancelled: () => boolean) {
  if (!crypto.subtle) throw new Error("Open DropHut over HTTPS to verify files for recovery.");
  const blockSize = 1024 * 1024;
  let digest = new Uint8Array(32);
  for (let offset = 0; offset < file.size; offset += blockSize) {
    if (cancelled()) throw new Error("File verification cancelled.");
    const block = new Uint8Array(await file.slice(offset, offset + blockSize).arrayBuffer());
    const input = new Uint8Array(digest.length + block.length);
    input.set(digest);
    input.set(block, digest.length);
    digest = new Uint8Array(await crypto.subtle.digest("SHA-256", input));
  }
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
