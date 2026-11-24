import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import pg from "pg";
import WebSocket from "ws";

async function startServer(t, databaseUrl, path = "") {
  const port = process.env.TEST_POSTGRES_HTTP_PORT ?? "18814";
  const base = "http://127.0.0.1:" + port;
  const child = spawn(process.execPath, ["server/server.js"], {
    env: {
      ...process.env, PORT: port, DATABASE_URL: databaseUrl,
      STATS_DB_PATH: path, STATS_VISITORS_SINCE: "2026-11-24",
    },
  });
  let logs = "";
  child.stderr.on("data", (data) => { logs += data; });
  child.stdout.on("data", (data) => { logs += data; });
  async function stop() {
    if (child.exitCode !== null || child.signalCode !== null) return;
    const exit = once(child, "exit");
    child.kill();
    await exit;
  }
  t.after(stop);
  await Promise.race([
    once(child.stdout, "data"),
    once(child, "exit").then(() => assert.fail("Test backend failed to start")),
  ]);
  return {
    base,
    stop,
    logs: () => logs,
    get: () => fetch(base + "/api/stats"),
    post: (route, id) => fetch(base + "/api/stats/" + route, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ id }),
    }),
  };
}

async function expectTotals(server, filesTransferred, uniqueVisitors) {
  const response = await server.get();
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { filesTransferred, uniqueVisitors, visitorsSince: "2026-11-24" });
}

test("an unavailable Postgres connection returns 503 without falling back to SQLite or blocking signaling", { timeout: 10_000 }, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "drophut-postgres-outage-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const secret = "disposable-password-for-log-check";
  const server = await startServer(t, "postgresql://user:" + secret + "@127.0.0.1:1/drophut_test?sslmode=disable",
    join(directory, "fallback.sqlite"));
  const stats = server.get();
  const socket = new WebSocket(server.base.replace("http", "ws"));
  t.after(() => socket.terminate());
  await once(socket, "open");
  const registered = once(socket, "message");
  socket.send(JSON.stringify({ type: "create-pipe", code: "123456", token: "a".repeat(64) }));
  assert.equal(JSON.parse((await registered)[0]).type, "pipe-created");
  const response = await stats;
  assert.equal(response.status, 503);
  assert.equal(await response.text(), "");
  const visitorId = "b".repeat(32);
  assert.equal((await server.post("visitors", visitorId)).status, 503);
  const closed = once(socket, "close");
  socket.close();
  await closed;
  await server.stop();
  assert.match(server.logs(), /\[stats\] Postgres: ECONNREFUSED/);
  assert.equal(server.logs().includes(secret), false);
  assert.equal(server.logs().includes(visitorId), false);
});

const databaseUrl = process.env.TEST_POSTGRES_URL;
test("Postgres migration is idempotent and concurrent reports stay deduplicated across backend restarts", {
  skip: !databaseUrl && "Set TEST_POSTGRES_URL to a fresh disposable drophut_test database",
  timeout: 15_000,
}, async (t) => {
  // The integration test inserts rows. Require an explicitly named, empty test database.
  assert.match(new URL(databaseUrl).pathname, /^\/drophut_test(?:_[a-z0-9]+)?$/);
  const client = new pg.Client({ connectionString: databaseUrl, ssl: true, connectionTimeoutMillis: 2500 });
  t.after(() => client.end().catch(() => {}));
  await client.connect();
  const { rows: [existing] } = await client.query("SELECT to_regnamespace('drophut_stats') AS schema");
  assert.equal(existing.schema, null, "Use a fresh disposable Postgres database");
  const migration = await readFile(new URL("../migrations/postgres/001-homepage-stats.sql", import.meta.url), "utf8");
  await client.query(migration);
  const server = await startServer(t, databaseUrl);
  await expectTotals(server, 1000, 0);

  const transfer = "12345678-1234-4234-8234-123456789012";
  const visitor = "a".repeat(32);
  const responses = await Promise.all(Array.from({ length: 8 }, () => [
    server.post("transfers", transfer), server.post("visitors", visitor),
  ]).flat());
  assert.ok(responses.every((response) => response.status === 200));
  await expectTotals(server, 1001, 1);
  await client.query(migration);
  await expectTotals(server, 1001, 1);

  await server.stop();
  const restarted = await startServer(t, databaseUrl);
  await expectTotals(restarted, 1001, 1);
  assert.equal((await restarted.post("transfers", transfer)).status, 200);
  assert.equal((await restarted.post("visitors", visitor)).status, 200);
  await expectTotals(restarted, 1001, 1);
});
