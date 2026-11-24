import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay, setImmediate as flush } from "node:timers/promises";
import { test } from "node:test";
import ts from "typescript";
import WebSocket from "ws";

async function startServer(t, path) {
  const port = process.env.TEST_STATS_PORT ?? "18813";
  const base = "http://127.0.0.1:" + port;
  const child = spawn(process.execPath, ["server/server.js"], {
    env: { ...process.env, DATABASE_URL: "", PORT: port, STATS_DB_PATH: path, STATS_VISITORS_SINCE: "2026-12-01" },
  });
  let diagnostics = "";
  child.stderr.on("data", (data) => { diagnostics += data; });
  async function stop() {
    if (child.exitCode !== null || child.signalCode !== null) return;
    const exit = once(child, "exit");
    child.kill();
    const forceStop = setTimeout(() => child.kill("SIGKILL"), 3000);
    try {
      const [, signal] = await exit;
      assert.notEqual(signal, "SIGKILL", "Stats server did not shut down");
    } finally {
      clearTimeout(forceStop);
    }
  }
  t.after(stop);
  await Promise.race([
    once(child.stdout, "data"),
    once(child, "exit").then(() => assert.fail("Stats server did not start: " + diagnostics)),
  ]);
  return {
    base,
    stop,
    get: () => fetch(base + "/api/stats"),
    post: (route, id) => fetch(base + "/api/stats/" + route, { method: "POST", body: JSON.stringify({ id }) }),
  };
}

async function totals(server, filesTransferred, uniqueVisitors) {
  const response = await server.get();
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { filesTransferred, uniqueVisitors, visitorsSince: "2026-12-01" });
}

function mockIdentityStorage(t) {
  const storage = new Map();
  let identity;
  let transactions = Promise.resolve();
  t.mock.method(console, "info", () => {});
  t.mock.method(console, "debug", () => {});
  const definitions = {
    localStorage: {
      getItem: (key) => storage.get(key) ?? null,
      setItem: (key, value) => storage.set(key, value),
    },
    indexedDB: {
      open() {
        const request = {
          result: {
            close() {},
            transaction() {
              const tx = {};
              const read = {};
              tx.objectStore = () => ({ get: () => read, put: (id) => { identity = id; } });
              transactions = transactions.then(() => {
                read.result = identity;
                read.onsuccess();
                tx.oncomplete();
              });
              return tx;
            },
          },
        };
        queueMicrotask(() => request.onsuccess());
        return request;
      },
    },
  };
  installGlobals(t, definitions);
  return storage;
}

function installGlobals(t, definitions) {
  for (const [name, value] of Object.entries(definitions)) {
    const previous = Object.getOwnPropertyDescriptor(globalThis, name);
    Object.defineProperty(globalThis, name, { value, configurable: true, writable: true });
    t.after(() => previous ? Object.defineProperty(globalThis, name, previous) : delete globalThis[name]);
  }
}

async function receiverHarness(t, events, reportCompletedFile) {
  const records = new Map();
  const batches = new Map();
  const peers = [];
  const effects = [];
  let notify;
  let downloads = 0;
  let cleanups = [];
  t.after(() => cleanups.forEach((cleanup) => cleanup()));
  const later = (callback, ms) => setTimeout(callback, ms).unref();
  installGlobals(t, {
    window: { setTimeout: later, clearTimeout },
    document: {
      body: { appendChild() {} },
      createElement: () => ({ style: {}, click: () => { downloads++; }, remove() {} }),
    },
    RTCPeerConnection: class {
      constructor() { peers.push(this); }
      connectionState = "connected";
      async setRemoteDescription(value) { this.remoteDescription = value; }
      async createAnswer() { return { type: "answer", sdp: "" }; }
      async setLocalDescription(value) { this.localDescription = value; }
      close() {}
    },
  });
  const session = { code: "123456", token: "private-token" };
  const dependencies = {
    "react": {
      useState: (initial) => [typeof initial === "function" ? initial() : initial, () => {}],
      useRef: (current) => ({ current }),
      useEffect: (effect) => effects.push(effect),
    },
    "react/jsx-runtime": { jsx() {}, jsxs() {} },
    "./statsClient": { reportCompletedFile },
    "./pipeSignaling": {
      readSession: () => session,
      PipeSignaling: class {
        session = session;
        constructor(_role, _state, onMessage) { notify = onMessage; }
        open() {}
        send() {}
        stop() {}
      },
    },
    "./pipeCheckpoint": {
      async assembleFile(key, type) { return new Blob(batches.get(key) ?? [], { type }); },
      async deleteSessionBatches() {},
      async loadCheckpoint(key) { return records.get(key); },
      async saveCheckpoint(key, record, batch) {
        records.set(key, record);
        if (batch) batches.set(key, [...(batches.get(key) ?? []), batch]);
      },
    },
  };
  const source = await readFile(new URL("../../src/components/JoinPipe.tsx", import.meta.url), "utf8");
  const compiled = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, target: ts.ScriptTarget.ES2022 },
  });
  const exports = {};
  new Function("require", "exports", compiled.outputText)((id) => dependencies[id], exports);
  exports.default({ onHome() {} });
  cleanups = effects.map((effect) => effect());
  return {
    downloads: () => downloads,
    async connect() {
      notify({ type: "webrtc-offer", offer: { type: "offer", sdp: "" } });
      await flush();
      const channel = {
        readyState: "open",
        close() { this.readyState = "closed"; },
        send(data) { events.push(JSON.parse(data)); },
      };
      peers.at(-1).ondatachannel({ channel });
      return async (message) => {
        channel.onmessage({ data: message instanceof ArrayBuffer ? message : JSON.stringify(message) });
        await flush();
      };
    },
  };
}

const CHUNK_SIZE = 16 * 1024;
function chunk(transferId, index, size = CHUNK_SIZE) {
  const id = new TextEncoder().encode(transferId);
  const frame = new ArrayBuffer(6 + id.length + size);
  const header = new DataView(frame);
  header.setUint16(0, id.length);
  header.setUint32(2, index);
  new Uint8Array(frame).set(id, 6);
  return frame;
}

test("homepage stats deduplicate browser sessions and completed receiver transfers across restarts", { timeout: 15_000 }, async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "drophut-stats-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const path = join(dir, "counts.sqlite");
  const server = await startServer(t, path);
  await totals(server, 1000, 0);

  const storage = mockIdentityStorage(t);
  const clientUrl = new URL("../../src/components/statsClient.ts", import.meta.url);
  const firstTab = await import(clientUrl.href + "?tab=first");
  const secondTab = await import(clientUrl.href + "?tab=second");
  const [first, second] = await Promise.all([firstTab.getVisitorId(), secondTab.getVisitorId()]);
  assert.match(first, /^[a-f0-9]{32}$/);
  assert.equal(first, second);
  assert.equal(await firstTab.getVisitorId(), first);
  assert.equal(storage.size, 1);
  const visitorResponses = await Promise.all(Array.from({ length: 8 }, () => server.post("visitors", first)));
  assert.ok(visitorResponses.every((response) => response.ok));
  await totals(server, 1000, 1);
  const blockedStorage = t.mock.method(localStorage, "getItem", () => {
    throw new Error("Storage disabled");
  });
  assert.equal(await firstTab.getVisitorId(), undefined);
  await totals(server, 1000, 1);
  blockedStorage.mock.restore();

  const events = [];
  const writes = [];
  const originalFetch = globalThis.fetch;
  let failFirstReport = true;
  let reportAttempts = 0;
  installGlobals(t, {
    fetch(url, options) {
      if (url instanceof URL && url.pathname === "/api/stats/transfers") {
        reportAttempts++;
        if (failFirstReport) {
          failFirstReport = false;
          return Promise.reject(new Error("Simulated analytics outage"));
        }
        const promise = originalFetch(server.base + url.pathname, options);
        writes.push(promise);
        return promise;
      }
      return originalFetch(url, options);
    },
  });
  const receiver = await receiverHarness(t, events, (id) => {
    assert.equal(events.at(-1).type, "file-received");
    assert.equal(events.at(-1).transferId, id);
    events.push({ type: "analytics", id });
    firstTab.reportCompletedFile(id);
  });
  let send = await receiver.connect();
  const offer = (id, chunks) => ({
    type: "file-start", transferId: id, name: "same.bin", mimeType: "application/octet-stream",
    size: chunks * CHUNK_SIZE, totalChunks: chunks, chunkSize: CHUNK_SIZE,
  });
  const finish = (offered) => send({ ...offered, type: "file-end" });

  const cancelled = offer(randomUUID(), 1);
  await send(cancelled);
  await send({ type: "file-cancel", transferId: cancelled.transferId });
  await finish(cancelled);
  const failed = offer(randomUUID(), 1);
  await send(failed);
  await send(chunk(failed.transferId, 0, 1));
  assert.ok(events.some((event) => event.type === "file-error"));
  await finish(failed);
  const resumed = offer(randomUUID(), 9);
  await send(resumed);
  for (let i = 0; i < 8; i++) await send(chunk(resumed.transferId, i));
  await finish(resumed);
  assert.equal(events.filter((event) => event.type === "analytics").length, 0);
  await totals(server, 1000, 1);

  send = await receiver.connect();
  await send(resumed);
  assert.equal(events.at(-1).nextChunk, 8);
  await send(chunk(resumed.transferId, 8));
  await finish(resumed);
  await finish(resumed);
  for (let i = 0; i < 2; i++) {
    const item = offer(randomUUID(), 1);
    await send(item);
    await send(chunk(item.transferId, 0));
    await finish(item);
  }
  assert.equal(receiver.downloads(), 3);
  await delay(1250);
  assert.equal(reportAttempts, 4);
  assert.ok((await Promise.all(writes)).every((response) => response.ok));
  assert.equal((await server.post("transfers", resumed.transferId)).status, 200);
  await totals(server, 1003, 1);
  assert.equal((await server.post("transfers", "123456:private-token")).status, 400);

  await server.stop();
  const restarted = await startServer(t, path);
  await totals(restarted, 1003, 1);
  await restarted.stop();

  const unavailable = await startServer(t, join(dir, "missing", "counts.sqlite"));
  assert.equal((await unavailable.get()).status, 503);
  const socket = new WebSocket(unavailable.base.replace("http", "ws"));
  t.after(() => socket.terminate());
  await once(socket, "open");
  const registered = once(socket, "message");
  socket.send(JSON.stringify({ type: "create-pipe", code: "123456", token: "a".repeat(64) }));
  assert.deepEqual(JSON.parse((await registered)[0]), { type: "pipe-created", code: "123456" });
  const peer = new WebSocket(unavailable.base.replace("http", "ws"));
  t.after(() => peer.terminate());
  await once(peer, "open");
  const joined = once(peer, "message");
  const ready = once(socket, "message");
  peer.send(JSON.stringify({ type: "join-pipe", code: "123456", token: "b".repeat(64) }));
  assert.equal(JSON.parse((await joined)[0]).type, "pipe-joined");
  assert.equal(JSON.parse((await ready)[0]).type, "peer-ready");

  const forwarded = new Promise((resolve) => {
    function onMessage(raw) {
      const message = JSON.parse(raw);
      if (message.type !== "webrtc-offer") return;
      peer.off("message", onMessage);
      resolve(message);
    }
    peer.on("message", onMessage);
  });
  const offerMessage = { type: "webrtc-offer", code: "123456", offer: { type: "offer", sdp: "test" } };
  socket.send(JSON.stringify(offerMessage));
  assert.deepEqual(await forwarded, offerMessage);
  socket.close();
  peer.close();
  await Promise.all([once(socket, "close"), once(peer, "close")]);
});
