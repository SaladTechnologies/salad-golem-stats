import 'dotenv/config';

export const config = {
  port: parseInt(process.env.PORT || '8000', 10),

  postgres: {
    host: process.env.POSTGRES_HOST || 'localhost',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    database: process.env.POSTGRES_DB || 'statsdb',
    user: process.env.POSTGRES_USER || 'devuser',
    password: process.env.POSTGRES_PASSWORD || 'devpass',
  },

  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    db: parseInt(process.env.REDIS_DB || '0', 10),
  },

  frontendOrigins: (process.env.FRONTEND_ORIGINS || 'http://localhost:5173').split(','),

  cacheTtl: {
    stats: parseInt(process.env.CACHE_TTL_STATS || '3600', 10),
    trends: parseInt(process.env.CACHE_TTL_TRENDS || '3600', 10),
    city_counts: parseInt(process.env.CACHE_TTL_CITY || '86400', 10),
    geo_counts: parseInt(process.env.CACHE_TTL_CITY || '86400', 10),
    transactions: parseInt(process.env.CACHE_TTL_TRANSACTIONS || '60', 10),
    gpu_stats: parseInt(process.env.CACHE_TTL_GPU || '3600', 10),
  },
} as const;

export type CacheKey = keyof typeof config.cacheTtl;
