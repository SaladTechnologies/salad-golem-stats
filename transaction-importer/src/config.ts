import 'dotenv/config';

// GLM token contract address on Polygon
const GLM_CONTRACT_POLYGON = '0x0B220b82F3eA3B7F6d9A1D8ab58930C064A2b5Bf';

export const config = {
  // Master wallet that funds requester wallets
  masterWallet: process.env.MASTER_WALLET_ADDRESS || '',

  // Etherscan V2 API configuration
  etherscan: {
    apiKey: process.env.ETHERSCAN_API_KEY || '',
    baseUrl: 'https://api.etherscan.io/v2/api',
    // Chain ID: 137 for Polygon, 1 for Ethereum mainnet
    chainId: parseInt(process.env.ETHERSCAN_CHAIN_ID || '137', 10),
    // Rate limit: 3 calls per second for free tier
    rateLimit: parseInt(process.env.ETHERSCAN_RATE_LIMIT || '3', 10),
  },

  // GLM token contract address on Polygon
  glmContract: process.env.GLM_CONTRACT_ADDRESS || GLM_CONTRACT_POLYGON,

  postgres: {
    host: process.env.POSTGRES_HOST || 'localhost',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    database: process.env.POSTGRES_DB || 'statsdb',
    user: process.env.POSTGRES_USER || 'devuser',
    password: process.env.POSTGRES_PASSWORD || 'devpass',
  },

  // How far back to look for transactions on first run (days)
  initialLookbackDays: parseInt(process.env.INITIAL_LOOKBACK_DAYS || '90', 10),
} as const;
