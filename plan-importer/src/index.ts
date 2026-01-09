import { fetchMixpanelJql } from './mixpanel.js';
import { importPlans } from './planner.js';
import { closePool } from './db.js';
import { logger } from './logger.js';
import { config } from './config.js';

let importInterval: NodeJS.Timeout | null = null;

// Handle graceful shutdown
process.on('SIGINT', () => shutdownHandler('SIGINT'));
process.on('SIGTERM', () => shutdownHandler('SIGTERM'));

async function shutdownHandler(signal: string): Promise<void> {
  logger.info(`Received ${signal}, shutting down...`);
  if (importInterval) {
    clearInterval(importInterval);
  }
  await closePool();
  process.exit(0);
}

async function runImportCycle(): Promise<void> {
  try {
    // Fetch data from MixPanel JQL API
    await fetchMixpanelJql();

    // Import plans to PostgreSQL
    await importPlans();
  } catch (error) {
    logger.error('Error during import cycle:', error);
  }
}

// Run initial import
logger.info('Starting plan-importer service...');
await runImportCycle();

// Schedule periodic imports
const intervalMs = config.importInterval;
const intervalHours = intervalMs / (1000 * 60 * 60);
logger.info(`Scheduling imports every ${intervalHours} hours`);

importInterval = setInterval(runImportCycle, intervalMs);
