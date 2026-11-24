import { Worker } from "node:worker_threads";
import { createPostgresStats } from "./stats-postgres.js";

const ID_PATTERN = /^(?:[a-f0-9]{4,32}|[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}|[0-9]{13}-[a-z0-9]{1,11})$/;
const VISITOR_PATTERN = /^[a-f0-9]{32}$/;
const DEFAULT_START = "2026-11-24";

function startDate(configured) {
  const date = new Date(configured + "T00:00:00Z");
  return /^\d{4}-\d{2}-\d{2}$/.test(configured) &&
    Number.isFinite(date.valueOf()) && date.toISOString().slice(0, 10) === configured ? configured : DEFAULT_START;
}

async function readId(request, pattern) {
  let body = "";
  for await (const chunk of request) {
    body += chunk;
    if (body.length > 256) return;
  }
  const data = JSON.parse(body);
  if (data && Object.keys(data).length === 1 && typeof data.id === "string" && pattern.test(data.id)) return data.id;
}

export function createStatsHandler({ databaseUrl, path, since = DEFAULT_START } = {}) {
  const postgres = databaseUrl ? createPostgresStats(databaseUrl) : undefined;
  let worker;
  let sequence = 0;
  const pending = new Map();

  function querySqlite(operation, id) {
    if (!path || pending.size >= 64) return Promise.resolve();
    return new Promise((resolve) => {
      if (!worker) {
        const active = new Worker(new URL("./stats-worker.js", import.meta.url), { workerData: { path } });
        worker = active;
        active.on("message", ({ request, totals }) => pending.get(request)?.(totals));
        const stopped = () => {
          if (worker !== active) return;
          worker = undefined;
          for (const finish of pending.values()) finish();
        };
        active.once("error", stopped);
        active.once("exit", stopped);
        active.unref();
      }
      const request = ++sequence;
      const active = worker;
      const timeout = setTimeout(() => {
        if (worker === active) void active.terminate();
        pending.get(request)?.();
      }, 2500);
      pending.set(request, (totals) => {
        clearTimeout(timeout);
        pending.delete(request);
        resolve(totals);
      });
      worker.postMessage({ request, operation, id });
    });
  }

  async function handleStats(request, response) {
    const route = request.url?.split("?")[0];
    const operation = {
      "/api/stats": "totals",
      "/api/stats/visitors": "visitor",
      "/api/stats/transfers": "transfer",
    }[route];
    const reply = (status, data) => {
      response.writeHead(status, {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Cache-Control": "no-store",
        "Content-Type": "application/json",
      });
      response.end(data === undefined ? undefined : JSON.stringify(data));
    };
    if (!operation) return reply(404);
    if (request.method === "OPTIONS") return reply(204);
    if (request.method !== (operation === "totals" ? "GET" : "POST")) return reply(405);

    let id;
    if (operation !== "totals") {
      try {
        id = await readId(request, operation === "visitor" ? VISITOR_PATTERN : ID_PATTERN);
      } catch {
        return reply(400);
      }
      if (!id) return reply(400);
    }

    try {
      const totals = await (postgres ? postgres.query(operation, id) : querySqlite(operation, id));
      if (totals) return reply(200, { ...totals, visitorsSince: startDate(since) });
    } catch {
      // Stats can be unavailable while the signaling endpoint continues serving.
    }
    reply(503);
  }

  handleStats.close = () => postgres?.close() ?? worker?.terminate();
  return handleStats;
}
