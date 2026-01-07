import { config } from '../config.js';
import { getPlanStats } from './planMetrics.js';
import { getRedisClient } from '../cache/redis.js';
import { createHash } from 'crypto';
import type { PlanPeriod } from '../types/index.js';

// Periods to keep warm
const PERIODS: PlanPeriod[] = ['6h', '24h', '7d', '30d', '90d', 'total'];

let warmInterval: NodeJS.Timeout | null = null;
let isWarming = false;

function generateCacheKey(period: string): string {
  const query = { period };
  const queryString = JSON.stringify(query, Object.keys(query).sort());
  const hash = createHash('md5').update(queryString).digest('hex');
  return `plan_stats:${hash}`;
}

async function warmCache(): Promise<void> {
  if (isWarming) {
    console.log('[CACHE WARMER] Already warming, skipping...');
    return;
  }

  isWarming = true;
  const startTime = Date.now();
  console.log('[CACHE WARMER] Starting cache warm...');

  const redis = getRedisClient();
  if (!redis) {
    console.error('[CACHE WARMER] Redis not available');
    isWarming = false;
    return;
  }

  const ttl = config.cacheTtl.plan_stats;

  for (const period of PERIODS) {
    try {
      const periodStart = Date.now();
      const result = await getPlanStats(period);
      const cacheKey = generateCacheKey(period);

      await redis.setEx(cacheKey, ttl, JSON.stringify(result));

      const elapsed = Date.now() - periodStart;
      console.log(`[CACHE WARMER] Warmed ${period} in ${elapsed}ms`);
    } catch (err) {
      console.error(`[CACHE WARMER] Failed to warm ${period}:`, err);
    }
  }

  const totalElapsed = Date.now() - startTime;
  console.log(`[CACHE WARMER] Completed in ${totalElapsed}ms`);
  isWarming = false;
}

export function startCacheWarmer(): void {
  if (!config.cacheWarmer.enabled) {
    console.log('[CACHE WARMER] Disabled via CACHE_WARMER_ENABLED=false');
    return;
  }

  const ttl = config.cacheTtl.plan_stats;
  const warmIntervalMs = Math.floor(ttl * config.cacheWarmer.intervalRatio * 1000);

  console.log(`[CACHE WARMER] Starting with ${warmIntervalMs / 1000}s interval (TTL: ${ttl}s)`);

  // Initial warm after a short delay (let server fully start)
  setTimeout(() => {
    warmCache();
  }, 5000);

  // Schedule periodic warming
  warmInterval = setInterval(() => {
    warmCache();
  }, warmIntervalMs);
}

export function stopCacheWarmer(): void {
  if (warmInterval) {
    clearInterval(warmInterval);
    warmInterval = null;
    console.log('[CACHE WARMER] Stopped');
  }
}

// Manual trigger for testing
export { warmCache };
