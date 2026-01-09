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
  // Single table for all GLM token transactions
  await pool.query(`
    CREATE TABLE IF NOT EXISTS glm_transactions (
      id SERIAL PRIMARY KEY,
      tx_hash TEXT UNIQUE NOT NULL,
      block_number BIGINT NOT NULL,
      block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
      from_address TEXT NOT NULL,
      to_address TEXT NOT NULL,
      value_wei TEXT NOT NULL,
      value_glm DOUBLE PRECISION NOT NULL,
      gas_used BIGINT,
      gas_price_wei TEXT,
      tx_type TEXT NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
  `);

  // Indexes for performance
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_glm_transactions_timestamp
    ON glm_transactions(block_timestamp DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_glm_transactions_from
    ON glm_transactions(from_address)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_glm_transactions_to
    ON glm_transactions(to_address)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_glm_transactions_type
    ON glm_transactions(tx_type)
  `);

  // Track import state (last processed block)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS import_state (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
  `);
}

export async function getLastProcessedBlock(): Promise<number | null> {
  const result = await pool.query(
    `SELECT value FROM import_state WHERE key = 'last_processed_block'`
  );
  if (result.rows.length === 0) {
    return null;
  }
  return parseInt(result.rows[0].value, 10);
}

export async function setLastProcessedBlock(blockNumber: number): Promise<void> {
  await pool.query(
    `INSERT INTO import_state (key, value, updated_at)
     VALUES ('last_processed_block', $1, NOW())
     ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()`,
    [blockNumber.toString()]
  );
}

export async function transactionExists(txHash: string): Promise<boolean> {
  const result = await pool.query(
    `SELECT 1 FROM glm_transactions WHERE tx_hash = $1`,
    [txHash]
  );
  return result.rows.length > 0;
}

export async function getExistingTxHashes(txHashes: string[]): Promise<Set<string>> {
  if (txHashes.length === 0) return new Set();

  const result = await pool.query(
    `SELECT tx_hash FROM glm_transactions WHERE tx_hash = ANY($1)`,
    [txHashes]
  );
  return new Set(result.rows.map(row => row.tx_hash));
}

export async function getRequesterWallets(): Promise<string[]> {
  const result = await pool.query(
    `SELECT DISTINCT to_address FROM glm_transactions WHERE tx_type = 'master_to_requester'`
  );
  return result.rows.map(row => row.to_address.toLowerCase());
}

export async function closePool(): Promise<void> {
  await pool.end();
}
