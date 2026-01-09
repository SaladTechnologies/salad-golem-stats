import http from 'node:http';
import { runImport } from './importer.js';
import { ensureTables, closePool } from './db.js';
import { logger } from './logger.js';
import { config } from './config.js';

let healthServer: http.Server;
let isHealthy = true;

// Parse command line arguments
const runOnce = process.argv.includes('--once');

// Handle graceful shutdown
process.on('SIGINT', () => shutdownHandler('SIGINT'));
process.on('SIGTERM', () => shutdownHandler('SIGTERM'));

async function shutdownHandler(signal: string): Promise<void> {
  logger.info(`Received ${signal}, shutting down...`);
  healthServer?.close();
  await closePool();
  process.exit(0);
}

async function runImportCycle(): Promise<void> {
  try {
    await runImport();
    isHealthy = true;
  } catch (error) {
    logger.error({ err: error }, 'Error during import cycle');
    isHealthy = false;
  }
}

// Start health check server
function startHealthServer(port: number): http.Server {
  const server = http.createServer((req, res) => {
    if (req.url === '/health') {
      if (isHealthy) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'healthy' }));
      } else {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'unhealthy' }));
      }
    } else {
      res.writeHead(404);
      res.end();
    }
  });

  server.listen(port, () => {
    logger.info(`Health check server listening on port ${port}`);
  });

  return server;
}

// Validate configuration
if (!config.masterWallet) {
  logger.error('MASTER_WALLET_ADDRESS environment variable is required');
  process.exit(1);
}

if (!config.etherscan.apiKey) {
  logger.error('ETHERSCAN_API_KEY environment variable is required');
  process.exit(1);
}

// Initialize
logger.info('Starting transaction-importer service...');
logger.info(`Master wallet: ${config.masterWallet}`);
logger.info(`Chain ID: ${config.etherscan.chainId}`);
logger.info(`GLM contract: ${config.glmContract}`);
if (runOnce) {
  logger.info('Running in --once mode (single import, then exit)');
}

// Start health check server
healthServer = startHealthServer(3001);

// Ensure database tables exist
await ensureTables();

if (runOnce) {
  // Single import mode
  await runImportCycle();
  logger.info('Single import complete, exiting');
  await closePool();
  healthServer?.close();
  process.exit(0);
}

// Run continuous import loop
logger.info('Running continuous import loop (rate-limited to 3 req/s)');

while (true) {
  await runImportCycle();
}
