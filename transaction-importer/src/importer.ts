import { DateTime } from 'luxon';
import { config } from './config.js';
import { logger } from './logger.js';
import {
  pool,
  getRequesterWallets,
  getExistingTxHashes,
  getWalletLastProcessedBlock,
  setWalletLastProcessedBlock,
  getWalletsLastProcessedBlocks,
} from './db.js';
import { getAllGlmTransfers, getBlockByTimestamp, TokenTransfer } from './etherscan.js';

// Transaction types for categorization
type TxType = 'master_to_requester' | 'requester_to_provider';

interface ProcessedTransaction {
  txHash: string;
  blockNumber: number;
  blockTimestamp: Date;
  fromAddress: string;
  toAddress: string;
  valueWei: string;
  valueGlm: number;
  gasUsed: number | null;
  gasPriceWei: string | null;
  txType: TxType;
}

const BATCH_SIZE = 100;

/**
 * Convert wei string to GLM (18 decimals)
 */
function weiToGlm(weiValue: string): number {
  const wei = BigInt(weiValue);
  const decimals = BigInt(10 ** 18);
  return Number(wei) / Number(decimals);
}

/**
 * Process a token transfer into our format
 */
function processTransfer(transfer: TokenTransfer, txType: TxType): ProcessedTransaction {
  return {
    txHash: transfer.hash,
    blockNumber: parseInt(transfer.blockNumber, 10),
    blockTimestamp: new Date(parseInt(transfer.timeStamp, 10) * 1000),
    fromAddress: transfer.from.toLowerCase(),
    toAddress: transfer.to.toLowerCase(),
    valueWei: transfer.value,
    valueGlm: weiToGlm(transfer.value),
    gasUsed: transfer.gasUsed ? parseInt(transfer.gasUsed, 10) : null,
    gasPriceWei: transfer.gasPrice || null,
    txType,
  };
}

/**
 * Insert transactions in batches for better performance
 */
async function insertTransactionBatch(transactions: ProcessedTransaction[]): Promise<number> {
  if (transactions.length === 0) return 0;

  let inserted = 0;

  for (let i = 0; i < transactions.length; i += BATCH_SIZE) {
    const batch = transactions.slice(i, i + BATCH_SIZE);

    // Build parameterized query for batch insert
    const values: unknown[] = [];
    const placeholders: string[] = [];

    batch.forEach((tx, idx) => {
      const offset = idx * 10;
      placeholders.push(
        `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10})`
      );
      values.push(
        tx.txHash,
        tx.blockNumber,
        tx.blockTimestamp,
        tx.fromAddress,
        tx.toAddress,
        tx.valueWei,
        tx.valueGlm,
        tx.gasUsed,
        tx.gasPriceWei,
        tx.txType
      );
    });

    const query = `
      INSERT INTO glm_transactions
      (tx_hash, block_number, block_timestamp, from_address, to_address,
       value_wei, value_glm, gas_used, gas_price_wei, tx_type)
      VALUES ${placeholders.join(', ')}
      ON CONFLICT (tx_hash) DO NOTHING
    `;

    const result = await pool.query(query, values);
    inserted += result.rowCount ?? 0;
  }

  return inserted;
}

/**
 * Import transactions from the master wallet
 * Discovers requester wallets by finding outgoing transfers
 * Uses per-wallet block tracking
 */
async function importMasterWalletTransactions(initialLookbackBlock: number): Promise<string[]> {
  const masterWallet = config.masterWallet.toLowerCase();

  if (!masterWallet) {
    throw new Error('MASTER_WALLET_ADDRESS not configured');
  }

  // Get the last processed block for the master wallet
  let startBlock = await getWalletLastProcessedBlock(masterWallet);

  if (startBlock === null) {
    startBlock = initialLookbackBlock;
    logger.info(`Master wallet: first run, starting from block ${startBlock}`);
  } else {
    logger.info(`Master wallet: continuing from block ${startBlock}`);
  }

  logger.info(`Fetching GLM transfers from master wallet: ${masterWallet}`);
  const transfers = await getAllGlmTransfers(masterWallet, startBlock);

  // Filter to outgoing transfers only
  const outgoingTransfers = transfers.filter(
    t => t.from.toLowerCase() === masterWallet
  );

  if (outgoingTransfers.length === 0) {
    logger.info('No outgoing transfers from master wallet');
    return [];
  }

  // Check which tx hashes already exist
  const txHashes = outgoingTransfers.map(t => t.hash);
  const existingHashes = await getExistingTxHashes(txHashes);

  // Process only new transfers
  const newTransfers = outgoingTransfers.filter(t => !existingHashes.has(t.hash));
  const processed = newTransfers.map(t => processTransfer(t, 'master_to_requester'));

  // Batch insert
  const inserted = await insertTransactionBatch(processed);

  // Update last processed block for master wallet
  const maxBlock = Math.max(...outgoingTransfers.map(t => parseInt(t.blockNumber, 10)));
  if (maxBlock > 0) {
    await setWalletLastProcessedBlock(masterWallet, maxBlock);
    logger.info(`Master wallet: updated last processed block to ${maxBlock}`);
  }

  // Collect discovered requester wallets
  const discoveredRequesterWallets = new Set(processed.map(t => t.toAddress));

  logger.info(`Imported ${inserted} master->requester transactions, discovered ${discoveredRequesterWallets.size} requester wallets`);
  return Array.from(discoveredRequesterWallets);
}

/**
 * Import transactions from requester wallets to providers
 * Uses per-wallet block tracking - new wallets get full lookback
 */
async function importRequesterWalletTransactions(
  requesterWallets: string[],
  initialLookbackBlock: number
): Promise<void> {
  const masterWallet = config.masterWallet.toLowerCase();
  const requesterSet = new Set(requesterWallets);
  let totalInserted = 0;

  // Get last processed blocks for all requester wallets in one query
  const walletBlocks = await getWalletsLastProcessedBlocks(requesterWallets);

  for (const requesterWallet of requesterWallets) {
    // Determine start block for this wallet
    let startBlock = walletBlocks.get(requesterWallet);

    if (startBlock === undefined) {
      // New wallet - use full lookback period
      startBlock = initialLookbackBlock;
      logger.info(`Requester wallet ${requesterWallet}: new wallet, starting from block ${startBlock}`);
    } else {
      logger.info(`Requester wallet ${requesterWallet}: continuing from block ${startBlock}`);
    }

    const transfers = await getAllGlmTransfers(requesterWallet, startBlock);

    // Filter to valid provider payments
    const validTransfers = transfers.filter(t => {
      const from = t.from.toLowerCase();
      const to = t.to.toLowerCase();

      // Must be outgoing from this requester
      if (from !== requesterWallet) return false;
      // Skip refunds to master
      if (to === masterWallet) return false;
      // // Skip internal transfers to other requesters
      // if (requesterSet.has(to)) return false;

      return true;
    });

    // Update last processed block even if no valid transfers
    // (we still processed this wallet's transfers from the API)
    if (transfers.length > 0) {
      const maxBlock = Math.max(...transfers.map(t => parseInt(t.blockNumber, 10)));
      if (maxBlock > 0) {
        await setWalletLastProcessedBlock(requesterWallet, maxBlock);
      }
    }

    if (validTransfers.length === 0) {
      logger.info(`Requester wallet ${requesterWallet}: 0 valid provider payments`);
      continue;
    }

    // Check which tx hashes already exist
    const txHashes = validTransfers.map(t => t.hash);
    const existingHashes = await getExistingTxHashes(txHashes);

    // Process only new transfers
    const newTransfers = validTransfers.filter(t => !existingHashes.has(t.hash));
    const processed = newTransfers.map(t => processTransfer(t, 'requester_to_provider'));

    // Batch insert
    const inserted = await insertTransactionBatch(processed);
    totalInserted += inserted;

    logger.info(`Requester wallet ${requesterWallet}: ${inserted} new transactions`);
  }

  logger.info(`Total: ${totalInserted} requester->provider transactions`);
}

/**
 * Run a full import cycle
 * Uses per-wallet block tracking to ensure new wallets get full lookback
 */
export async function runImport(): Promise<void> {
  logger.info('Starting transaction import cycle');

  // Calculate the initial lookback block (used for new wallets)
  const lookbackTimestamp = Math.floor(
    DateTime.now().minus({ days: config.initialLookbackDays }).toSeconds()
  );
  const initialLookbackBlock = await getBlockByTimestamp(lookbackTimestamp);
  logger.info(`Initial lookback block: ${initialLookbackBlock} (${config.initialLookbackDays} days ago)`);

  // Step 1: Import master wallet transactions and discover requester wallets
  await importMasterWalletTransactions(initialLookbackBlock);

  // Step 2: Get all known requester wallets from existing transactions
  const allRequesterWallets = await getRequesterWallets();
  logger.info(`Total known requester wallets: ${allRequesterWallets.length}`);

  // Step 3: Import requester wallet transactions to providers
  // Each wallet uses its own last processed block, or initialLookbackBlock if new
  await importRequesterWalletTransactions(allRequesterWallets, initialLookbackBlock);

  logger.info('Import cycle complete');
}
