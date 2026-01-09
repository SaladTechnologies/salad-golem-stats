import fs from 'fs/promises';
import path from 'path';
import { config } from './config.js';
import { pool, ensureTables } from './db.js';
import { logger } from './logger.js';

// JSON array keys from MixPanel groupBy results
const JSON_KEYS = {
  ORG_NAME: 0,
  NODE_ID: 2,
  START_AT: 0,
  STOP_AT: 1,
  INVOICE_AMOUNT: 2,
  CPU: 3,
  RAM: 4,
  GPU_CLASS_ID: 5,
} as const;

interface MixpanelRow {
  key: [string, string, string]; // [org_name, container_group_slug, node_id]
  value: [number, number, number, number, number, string | null]; // [start_at, stop_at, invoice_amount, cpu, ram, gpu_class_id]
}

/**
 * Import plans from pending JSON files into PostgreSQL.
 */
export async function importPlans(): Promise<void> {
  // Ensure tables exist
  await ensureTables();

  const pendingDir = path.join(config.dataDirectory, 'pending');
  const importedDir = path.join(config.dataDirectory, 'imported');
  const failedDir = path.join(config.dataDirectory, 'failed');

  await fs.mkdir(pendingDir, { recursive: true });
  await fs.mkdir(importedDir, { recursive: true });
  await fs.mkdir(failedDir, { recursive: true });

  // Read JSON files
  let files: string[];
  try {
    files = (await fs.readdir(pendingDir)).filter((f) => f.endsWith('.json'));
  } catch {
    files = [];
  }

  logger.info(`Found ${files.length} JSON files to process.`);

  for (const jsonFile of files) {
    logger.info(`Processing file: ${jsonFile}`);
    const jsonFilePath = path.join(pendingDir, jsonFile);

    // Parse JSON
    let jsonData: MixpanelRow[];
    try {
      const fileContent = await fs.readFile(jsonFilePath, 'utf-8');
      jsonData = JSON.parse(fileContent);
    } catch (err) {
      logger.error(`Failed to parse JSON in ${jsonFile}:`, err);
      await fs.rename(jsonFilePath, path.join(failedDir, jsonFile));
      continue;
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Insert JSON file record
      const jsonFileResult = await client.query<{ id: number }>(
        'INSERT INTO json_import_file (file_name) VALUES ($1) RETURNING id',
        [jsonFile]
      );
      const jsonFileId = jsonFileResult.rows[0].id;

      // Process each row
      let importedCount = 0;
      let skippedCount = 0;

      for (const row of jsonData) {
        const startAt = row.value[JSON_KEYS.START_AT];
        const stopAt = row.value[JSON_KEYS.STOP_AT];
        const duration = stopAt - startAt;

        // Skip if less than minimum duration
        if (duration < config.minimumDuration) {
          skippedCount++;
          continue;
        }

        const invoiceAmount = row.value[JSON_KEYS.INVOICE_AMOUNT];
        const usdPerHour = (invoiceAmount / duration) * 3600000;

        await client.query(
          `INSERT INTO node_plan (
            org_name, node_id, json_import_file_id, start_at, stop_at,
            invoice_amount, usd_per_hour, gpu_class_id, ram, cpu
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
          [
            row.key[JSON_KEYS.ORG_NAME],
            row.key[JSON_KEYS.NODE_ID],
            jsonFileId,
            startAt,
            stopAt,
            invoiceAmount,
            usdPerHour,
            row.value[JSON_KEYS.GPU_CLASS_ID],
            row.value[JSON_KEYS.RAM],
            row.value[JSON_KEYS.CPU],
          ]
        );

        importedCount++;
      }

      await client.query('COMMIT');
      logger.info(
        `${jsonFile} processed: ${importedCount} rows imported, ${skippedCount} skipped (below minimum duration)`
      );

      // Move to imported folder
      await fs.rename(jsonFilePath, path.join(importedDir, jsonFile));
    } catch (err) {
      await client.query('ROLLBACK');
      logger.error(`Error importing ${jsonFile}:`, err);

      // Move to failed folder
      await fs.rename(jsonFilePath, path.join(failedDir, jsonFile));
    } finally {
      client.release();
    }
  }
}
