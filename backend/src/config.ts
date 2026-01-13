import 'dotenv/config';

export const config = {
  port: parseInt(process.env.PORT || '8000', 10),

  postgres: {
    host: process.env.POSTGRES_HOST || 'localhost',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    database: process.env.POSTGRES_DB || 'statsdb',
    user: process.env.POSTGRES_USER || 'devuser',
    password: process.env.POSTGRES_PASSWORD || 'devpass',
    ssl: process.env.POSTGRES_SSL === 'true',
  },

  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    db: parseInt(process.env.REDIS_DB || '0', 10),
    username: process.env.REDIS_USERNAME || undefined,
    password: process.env.REDIS_PASSWORD || undefined,
    tls: process.env.REDIS_TLS === 'true',
  },

  frontendOrigins: (process.env.FRONTEND_ORIGINS || 'http://localhost:5173').split(','),

  cacheTtl: {
    geo_counts: parseInt(process.env.CACHE_TTL_GEO || '86400', 10),
    transactions: parseInt(process.env.CACHE_TTL_TRANSACTIONS || '60', 10),
    plan_stats: parseInt(process.env.CACHE_TTL_PLAN_STATS || '3600', 10),
  },

  cacheWarmer: {
    enabled: process.env.CACHE_WARMER_ENABLED !== 'false', // Enabled by default
    intervalRatio: parseFloat(process.env.CACHE_WARMER_INTERVAL_RATIO || '0.8'), // Warm at 80% of TTL
  },

  // Minimum date for queried transactions (ISO 8601 format)
  // Transactions before this date will not be returned
  transactionsMinDate: process.env.TRANSACTIONS_MIN_DATE || '2026-01-01T00:00:00.000Z',
} as const;

export type CacheKey = keyof typeof config.cacheTtl;
