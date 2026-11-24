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

export function createPostgresStats(connectionString) {
  let pool;
  let pending = 0;

  async function query(operation, id) {
    if (pending >= 24) return;
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

    pending++;
    let client;
    let failed = false;
    try {
      client = await pool.connect();
      if (operation !== "totals") await client.query(INSERT[operation], [id]);
      const { rows: [row] } = await client.query(TOTALS);
      if (!row) return;
      const filesTransferred = Number(row.filesTransferred);
      const uniqueVisitors = Number(row.uniqueVisitors);
      if (Number.isSafeInteger(filesTransferred) && Number.isSafeInteger(uniqueVisitors)) {
        return { filesTransferred, uniqueVisitors };
      }
    } catch {
      failed = true;
    } finally {
      client?.release(failed);
      pending--;
    }
  }

  return { query, close: () => pool?.end() };
}
