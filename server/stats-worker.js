import { readFileSync } from "node:fs";
import { parentPort, workerData } from "node:worker_threads";

let database;
let statements;

async function open() {
  if (statements) return statements;
  const { DatabaseSync } = await import("node:sqlite");
  const db = new DatabaseSync(workerData.path);
  try {
    db.exec("PRAGMA busy_timeout = 1500");
    db.exec(readFileSync(new URL("./migrations/001-homepage-stats.sql", import.meta.url), "utf8"));
    const queries = {
      visitor: db.prepare("INSERT INTO unique_visitors (id) VALUES (?) ON CONFLICT (id) DO NOTHING"),
      transfer: db.prepare("INSERT INTO completed_transfers (id) VALUES (?) ON CONFLICT (id) DO NOTHING"),
      totals: db.prepare(`
        SELECT (SELECT total FROM stats_baseline WHERE name = 'files')
          + (SELECT COUNT(*) FROM completed_transfers) AS filesTransferred,
          (SELECT COUNT(*) FROM unique_visitors) AS uniqueVisitors
      `),
    };
    database = db;
    statements = queries;
    return statements;
  } catch (error) {
    db.close();
    throw error;
  }
}

let jobs = Promise.resolve();
parentPort.on("message", ({ request, operation, id }) => {
  jobs = jobs.then(async () => {
    try {
      const queries = await open();
      if (operation !== "totals") queries[operation].run(id);
      parentPort.postMessage({ request, totals: queries.totals.get() });
    } catch {
      database?.close();
      database = undefined;
      statements = undefined;
      parentPort.postMessage({ request });
    }
  });
});
