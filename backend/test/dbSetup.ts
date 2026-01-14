import { Pool } from 'pg';
import { readFile } from 'fs/promises';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Test database connection
export const testPool = new Pool({
  host: process.env.TEST_POSTGRES_HOST || 'localhost',
  port: parseInt(process.env.TEST_POSTGRES_PORT || '5432', 10),
  database: process.env.TEST_POSTGRES_DB || 'statsdb_test',
  user: process.env.TEST_POSTGRES_USER || 'devuser',
  password: process.env.TEST_POSTGRES_PASSWORD || 'devpass',
});

export async function setupTestDatabase(): Promise<void> {
  // Run migrations
  const migrationPath = join(__dirname, '..', '..', 'db', 'migrations', '001_init.sql');
  const migrationSQL = await readFile(migrationPath, 'utf-8');
  await testPool.query(migrationSQL);
}

export async function teardownTestDatabase(): Promise<void> {
  // Drop all tables
  await testPool.query(`
    DROP TABLE IF EXISTS node_plan CASCADE;
    DROP TABLE IF EXISTS json_import_file CASCADE;
    DROP TABLE IF EXISTS glm_transactions CASCADE;
    DROP TABLE IF EXISTS gpu_classes CASCADE;
  `);
}

export async function cleanupTestData(): Promise<void> {
  // Clean all data but keep tables
  await testPool.query('DELETE FROM node_plan');
  await testPool.query('DELETE FROM json_import_file');
  await testPool.query('DELETE FROM glm_transactions');
  await testPool.query('DELETE FROM gpu_classes');
}

export async function closeTestDatabase(): Promise<void> {
  await testPool.end();
}

interface NodePlanRow {
  org_name: string;
  node_id: string;
  start_at: number;
  stop_at: number | null;
  invoice_amount: number;
  usd_per_hour: number;
  gpu_class_id?: string | null;
  ram: number;
  cpu: number;
}

export async function insertTestNodePlans(plans: NodePlanRow[]): Promise<void> {
  const client = await testPool.connect();
  try {
    await client.query('BEGIN');

    // Insert a dummy json_import_file record
    const fileResult = await client.query<{ id: number }>(
      'INSERT INTO json_import_file (file_name) VALUES ($1) RETURNING id',
      ['test-data.json']
    );
    const jsonFileId = fileResult.rows[0].id;

    // Insert node plans
    for (const plan of plans) {
      await client.query(
        `INSERT INTO node_plan (
          org_name, node_id, json_import_file_id, start_at, stop_at,
          invoice_amount, usd_per_hour, gpu_class_id, ram, cpu
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        [
          plan.org_name,
          plan.node_id,
          jsonFileId,
          plan.start_at,
          plan.stop_at,
          plan.invoice_amount,
          plan.usd_per_hour,
          plan.gpu_class_id || null,
          plan.ram,
          plan.cpu,
        ]
      );
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

export async function insertTestGpuClasses(
  gpuClasses: Array<{ gpu_class_id: string; gpu_class_name: string; vram_gb: number }>
): Promise<void> {
  const client = await testPool.connect();
  try {
    await client.query('BEGIN');

    for (const gpu of gpuClasses) {
      await client.query(
        `INSERT INTO gpu_classes (gpu_class_id, gpu_class_name, vram_gb)
         VALUES ($1, $2, $3)
         ON CONFLICT (gpu_class_id) DO NOTHING`,
        [gpu.gpu_class_id, gpu.gpu_class_name, gpu.vram_gb]
      );
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}
