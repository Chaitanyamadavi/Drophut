import pg from "pg";

const INSERT = {
  visitor: "INSERT INTO drophut_stats.unique_visitors (id) VALUES ($1) ON CONFLICT (id) DO NOTHING",
  transfer: "INSERT INTO drophut_stats.completed_transfers (id) VALUES ($1) ON CONFLICT (id) DO NOTHING",
};

const TOTALS = `
  SELECT baseline.total + (SELECT count(*) FROM drophut_stats.completed_transfers) AS "filesTransferred",
    (SELECT count(*) FROM drophut_stats.unique_visitors) AS "uniqueVisitors"
  FROM drophut_stats.stats_baseline AS baseline
  WHERE baseline.name = 'files'
`;

const CONNECTION_CODES = new Set([
  "ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "ENOTFOUND", "EAI_AGAIN",
  "ERR_INVALID_URL", "ERR_TLS_CERT_ALTNAME_INVALID", "CERT_HAS_EXPIRED",
  "SELF_SIGNED_CERT_IN_CHAIN", "DEPTH_ZERO_SELF_SIGNED_CERT",
  "UNABLE_TO_VERIFY_LEAF_SIGNATURE", "UNABLE_TO_GET_ISSUER_CERT_LOCALLY",
]);

function failureCode(error, connected) {
  const code = error?.code;
  if (CONNECTION_CODES.has(code) ||
      (error instanceof pg.DatabaseError && typeof code === "string" && /^[0-9A-Z]{5}$/.test(code))) {
    return code;
  }
  return connected ? "query-failed" : "connection-failed";
}

export function createPostgresStats(connectionString) {
  let pool;
  let pending = 0;

  async function query(operation, id) {
    if (pending >= 24) return;
    pending++;
    let client;
    let failed = false;
    try {
      if (!pool) {
        pool = new pg.Pool({
          connectionString,
          max: 3,
          ssl: true,
          connectionTimeoutMillis: 1800,
          idleTimeoutMillis: 10_000,
          query_timeout: 2400,
          allowExitOnIdle: true,
        });
        // Idle clients can disconnect independently of HTTP requests. Never log raw driver errors.
        pool.on("error", () => {});
      }
      client = await pool.connect();
      if (operation !== "totals") await client.query(INSERT[operation], [id]);
      const { rows: [row] } = await client.query(TOTALS);
      if (!row) {
        console.error("[stats] Postgres: missing-baseline");
        return;
      }
      const filesTransferred = Number(row.filesTransferred);
      const uniqueVisitors = Number(row.uniqueVisitors);
      if (Number.isSafeInteger(filesTransferred) && Number.isSafeInteger(uniqueVisitors)) {
        return { filesTransferred, uniqueVisitors };
      }
      console.error("[stats] Postgres: invalid-totals");
    } catch (error) {
      failed = true;
      console.error("[stats] Postgres:", failureCode(error, Boolean(client)));
    } finally {
      client?.release(failed);
      pending--;
    }
  }

  return { query, close: () => pool?.end() };
}
