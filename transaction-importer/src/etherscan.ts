import { config } from './config.js';
import { logger } from './logger.js';

export interface TokenTransfer {
  blockNumber: string;
  timeStamp: string;
  hash: string;
  from: string;
  to: string;
  value: string;
  tokenName: string;
  tokenSymbol: string;
  tokenDecimal: string;
  gas: string;
  gasPrice: string;
  gasUsed: string;
}

interface EtherscanResponse {
  status: string;
  message: string;
  result: TokenTransfer[] | string;
}

// Rate limiter: tracks request timestamps to enforce rate limit
class RateLimiter {
  private timestamps: number[] = [];
  private readonly maxRequests: number;
  private readonly windowMs: number = 1000; // 1 second window

  constructor(maxRequestsPerSecond: number) {
    this.maxRequests = maxRequestsPerSecond;
  }

  async waitForSlot(): Promise<void> {
    const now = Date.now();

    // Remove timestamps older than the window
    this.timestamps = this.timestamps.filter(ts => now - ts < this.windowMs);

    if (this.timestamps.length >= this.maxRequests) {
      // Wait until the oldest request falls outside the window
      const oldestTimestamp = this.timestamps[0];
      const waitTime = this.windowMs - (now - oldestTimestamp) + 10; // +10ms buffer
      if (waitTime > 0) {
        logger.debug(`Rate limiting: waiting ${waitTime}ms`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
      }
      // Recursively check again after waiting
      return this.waitForSlot();
    }

    this.timestamps.push(Date.now());
  }
}

const rateLimiter = new RateLimiter(config.etherscan.rateLimit);

async function fetchWithRetry(url: string, retries: number = 3): Promise<EtherscanResponse> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    await rateLimiter.waitForSlot();

    try {
      const response = await fetch(url);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json() as EtherscanResponse;

      // Check for rate limit error in response
      if (data.status === '0' && typeof data.result === 'string' &&
          data.result.includes('rate limit')) {
        logger.warn(`Rate limited by Etherscan, waiting before retry...`);
        await new Promise(resolve => setTimeout(resolve, 1000));
        continue;
      }

      return data;
    } catch (error) {
      if (attempt === retries) {
        throw error;
      }
      logger.warn({ err: error }, `Attempt ${attempt} failed, retrying...`);
      await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
    }
  }

  throw new Error('All retries exhausted');
}

/**
 * Build URL with Etherscan V2 API format (includes chainid)
 */
function buildUrl(params: URLSearchParams): string {
  params.set('chainid', config.etherscan.chainId.toString());
  params.set('apikey', config.etherscan.apiKey);
  return `${config.etherscan.baseUrl}?${params.toString()}`;
}

/**
 * Get ERC-20 token transfers for an address
 */
export async function getTokenTransfers(
  address: string,
  options: {
    contractAddress?: string;
    startBlock?: number;
    endBlock?: number;
    page?: number;
    offset?: number;
    sort?: 'asc' | 'desc';
  } = {}
): Promise<TokenTransfer[]> {
  const params = new URLSearchParams({
    module: 'account',
    action: 'tokentx',
    address: address,
    sort: options.sort || 'asc',
  });

  if (options.contractAddress) {
    params.set('contractaddress', options.contractAddress);
  }
  if (options.startBlock !== undefined) {
    params.set('startblock', options.startBlock.toString());
  }
  if (options.endBlock !== undefined) {
    params.set('endblock', options.endBlock.toString());
  }
  if (options.page !== undefined) {
    params.set('page', options.page.toString());
  }
  if (options.offset !== undefined) {
    params.set('offset', options.offset.toString());
  }

  const url = buildUrl(params);
  logger.debug(`Fetching token transfers for ${address}`);

  const data = await fetchWithRetry(url);

  if (data.status === '0') {
    if (data.message === 'No transactions found' ||
        (typeof data.result === 'string' && data.result.includes('No transactions'))) {
      return [];
    }
    throw new Error(`Etherscan API error: ${data.message} - ${data.result}`);
  }

  if (!Array.isArray(data.result)) {
    logger.warn({ result: data.result }, `Unexpected response format`);
    return [];
  }

  return data.result;
}

/**
 * Get all GLM token transfers for an address (handles pagination)
 */
export async function getAllGlmTransfers(
  address: string,
  startBlock?: number
): Promise<TokenTransfer[]> {
  const allTransfers: TokenTransfer[] = [];
  let page = 1;
  const pageSize = 1000; // Max allowed by Etherscan

  while (true) {
    const transfers = await getTokenTransfers(address, {
      contractAddress: config.glmContract,
      startBlock,
      page,
      offset: pageSize,
      sort: 'asc',
    });

    if (transfers.length === 0) {
      break;
    }

    allTransfers.push(...transfers);
    logger.info(`Fetched page ${page}: ${transfers.length} transfers (total: ${allTransfers.length})`);

    if (transfers.length < pageSize) {
      break;
    }

    page++;
  }

  return allTransfers;
}

/**
 * Get current block number
 */
export async function getCurrentBlockNumber(): Promise<number> {
  const params = new URLSearchParams({
    module: 'proxy',
    action: 'eth_blockNumber',
  });

  const url = buildUrl(params);

  await rateLimiter.waitForSlot();
  const response = await fetch(url);
  const data = await response.json() as { result: string };

  return parseInt(data.result, 16);
}

/**
 * Get block number for a given timestamp
 */
export async function getBlockByTimestamp(
  timestamp: number,
  closest: 'before' | 'after' = 'before'
): Promise<number> {
  const params = new URLSearchParams({
    module: 'block',
    action: 'getblocknobytime',
    timestamp: timestamp.toString(),
    closest,
  });

  const url = buildUrl(params);

  await rateLimiter.waitForSlot();
  const response = await fetch(url);
  const data = await response.json() as { result: string };

  return parseInt(data.result, 10);
}
