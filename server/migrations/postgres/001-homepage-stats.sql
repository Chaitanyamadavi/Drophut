BEGIN;

CREATE SCHEMA IF NOT EXISTS drophut_stats;

CREATE TABLE IF NOT EXISTS drophut_stats.stats_baseline (
  name text PRIMARY KEY,
  total integer NOT NULL
);

INSERT INTO drophut_stats.stats_baseline (name, total) VALUES ('files', 1000)
ON CONFLICT (name) DO NOTHING;

CREATE TABLE IF NOT EXISTS drophut_stats.completed_transfers (id text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS drophut_stats.unique_visitors (id text PRIMARY KEY);

COMMIT;
