import { config } from '../config.js';
import { getPlanStats } from './planMetrics.js';
import { getGolemNetworkStats, getGolemHistoricalStats } from './golemMetrics.js';
import { getRedisClient, generateCacheKeyForWarmer } from '../cache/redis.js';
import type { PlanPeriod } from '../types/index.js';

// Periods to keep warm
const PERIODS: PlanPeriod[] = ['6h', '24h', '7d', '30d', '90d', 'total'];

let planStatsInterval: NodeJS.Timeout | null = null;
let golemStatsInterval: NodeJS.Timeout | null = null;
let golemHistoricalInterval: NodeJS.Timeout | null = null;

let isPlanStatsWarming = false;
let isGolemStatsWarming = false;
let isGolemHistoricalWarming = false;

async function warmPlanStats(): Promise<void> {
  if (isPlanStatsWarming) {
    console.log('[CACHE WARMER] Plan stats already warming, skipping...');
    return;
  }

  isPlanStatsWarming = true;
  const startTime = Date.now();
  console.log('[CACHE WARMER] Warming plan stats...');

  const redis = getRedisClient();
  if (!redis) {
    console.error('[CACHE WARMER] Redis not available');
    isPlanStatsWarming = false;
    return;
  }

  const ttl = config.cacheTtl.plan_stats;

  for (const period of PERIODS) {
    try {
      const periodStart = Date.now();
      const result = await getPlanStats(period);
      const cacheKey = generateCacheKeyForWarmer('plan_stats', { period });

      await redis.setEx(cacheKey, ttl, JSON.stringify(result));

      const elapsed = Date.now() - periodStart;
      console.log(`[CACHE WARMER] Warmed plan_stats:${period} in ${elapsed}ms`);
    } catch (err) {
      console.error(`[CACHE WARMER] Failed to warm plan_stats:${period}:`, err);
    }
  }

  const totalElapsed = Date.now() - startTime;
  console.log(`[CACHE WARMER] Plan stats completed in ${totalElapsed}ms`);
  isPlanStatsWarming = false;
}

async function warmGolemStats(): Promise<void> {
  if (isGolemStatsWarming) {
    console.log('[CACHE WARMER] Golem stats already warming, skipping...');
    return;
  }

  isGolemStatsWarming = true;
  const startTime = Date.now();

  const redis = getRedisClient();
  if (!redis) {
    console.error('[CACHE WARMER] Redis not available');
    isGolemStatsWarming = false;
    return;
  }

  try {
    const golemStats = await getGolemNetworkStats();
    const cacheKey = generateCacheKeyForWarmer('golem_stats');
    const ttl = config.cacheTtl.golem_stats;

    await redis.setEx(cacheKey, ttl, JSON.stringify(golemStats));

    const elapsed = Date.now() - startTime;
    console.log(`[CACHE WARMER] Warmed golem_stats in ${elapsed}ms`);
  } catch (err) {
    console.error('[CACHE WARMER] Failed to warm golem_stats:', err);
  }

  isGolemStatsWarming = false;
}

async function warmGolemHistorical(): Promise<void> {
  if (isGolemHistoricalWarming) {
    console.log('[CACHE WARMER] Golem historical already warming, skipping...');
    return;
  }

  isGolemHistoricalWarming = true;
  const startTime = Date.now();

  const redis = getRedisClient();
  if (!redis) {
    console.error('[CACHE WARMER] Redis not available');
    isGolemHistoricalWarming = false;
    return;
  }

  try {
    const golemHist = await getGolemHistoricalStats();
    const cacheKey = generateCacheKeyForWarmer('golem_historical');
    const ttl = config.cacheTtl.golem_historical;

    await redis.setEx(cacheKey, ttl, JSON.stringify(golemHist));

    const elapsed = Date.now() - startTime;
    console.log(`[CACHE WARMER] Warmed golem_historical in ${elapsed}ms`);
  } catch (err) {
    console.error('[CACHE WARMER] Failed to warm golem_historical:', err);
  }

  isGolemHistoricalWarming = false;
}

// Warm all caches once (for initial startup)
async function warmAllCaches(): Promise<void> {
  console.log('[CACHE WARMER] Initial warm of all caches...');
  await Promise.all([
    warmPlanStats(),
    warmGolemStats(),
    warmGolemHistorical(),
  ]);
}

export function startCacheWarmer(): void {
  if (!config.cacheWarmer.enabled) {
    console.log('[CACHE WARMER] Disabled via CACHE_WARMER_ENABLED=false');
    return;
  }

  // Calculate intervals for each cache type
  const planStatsIntervalMs = Math.floor(
    config.cacheTtl.plan_stats * config.cacheWarmer.intervalRatio * 1000
  );
  const golemStatsIntervalMs = Math.floor(
    config.cacheTtl.golem_stats * config.cacheWarmer.intervalRatio * 1000
  );
  const golemHistoricalIntervalMs = Math.floor(
    config.cacheTtl.golem_historical * config.cacheWarmer.intervalRatio * 1000
  );

  console.log('[CACHE WARMER] Starting with intervals:');
  console.log(`  - Plan stats: every ${planStatsIntervalMs / 1000}s (TTL: ${config.cacheTtl.plan_stats}s)`);
  console.log(`  - Golem stats: every ${golemStatsIntervalMs / 1000}s (TTL: ${config.cacheTtl.golem_stats}s)`);
  console.log(`  - Golem historical: every ${golemHistoricalIntervalMs / 1000}s (TTL: ${config.cacheTtl.golem_historical}s)`);

  // Initial warm after a short delay (let server fully start)
  setTimeout(() => {
    warmAllCaches();
  }, 5000);

  // Schedule periodic warming for each cache type
  planStatsInterval = setInterval(() => {
    warmPlanStats();
  }, planStatsIntervalMs);

  golemStatsInterval = setInterval(() => {
    warmGolemStats();
  }, golemStatsIntervalMs);

  golemHistoricalInterval = setInterval(() => {
    warmGolemHistorical();
  }, golemHistoricalIntervalMs);
}

export function stopCacheWarmer(): void {
  if (planStatsInterval) {
    clearInterval(planStatsInterval);
    planStatsInterval = null;
  }
  if (golemStatsInterval) {
    clearInterval(golemStatsInterval);
    golemStatsInterval = null;
  }
  if (golemHistoricalInterval) {
    clearInterval(golemHistoricalInterval);
    golemHistoricalInterval = null;
  }
  console.log('[CACHE WARMER] Stopped all warming loops');
}

// Check if all required cache keys exist
export async function isCacheReady(): Promise<boolean> {
  const redis = getRedisClient();
  if (!redis) {
    return false;
  }

  const planStatsCacheKeys = PERIODS.map((period) =>
    generateCacheKeyForWarmer('plan_stats', { period })
  );
  const golemCacheKeys = [
    generateCacheKeyForWarmer('golem_stats'),
    generateCacheKeyForWarmer('golem_historical'),
  ];

  const allCacheKeys = [...planStatsCacheKeys, ...golemCacheKeys];
  const count = await redis.exists(allCacheKeys);
  return count === allCacheKeys.length;
}

// Manual trigger for testing
export { warmAllCaches as warmCache };
