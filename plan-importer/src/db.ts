import pg from 'pg';
import { config } from './config.js';

const { Pool } = pg;

export const pool = new Pool({
  host: config.postgres.host,
  port: config.postgres.port,
  database: config.postgres.database,
  user: config.postgres.user,
  password: config.postgres.password,
});

export async function ensureTables(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS json_import_file (
      id SERIAL PRIMARY KEY,
      file_name TEXT UNIQUE
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS node_plan (
      id SERIAL PRIMARY KEY,
      org_name TEXT,
      node_id TEXT,
      json_import_file_id INTEGER REFERENCES json_import_file(id),
      start_at BIGINT,
      stop_at BIGINT,
      invoice_amount DOUBLE PRECISION,
      usd_per_hour DOUBLE PRECISION,
      gpu_class_id TEXT,
      ram DOUBLE PRECISION,
      cpu DOUBLE PRECISION
    )
  `);

  // Create index on json_import_file_id for faster lookups
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_node_plan_json_import_file_id
    ON node_plan(json_import_file_id)
  `);
}

export async function closePool(): Promise<void> {
  await pool.end();
}
