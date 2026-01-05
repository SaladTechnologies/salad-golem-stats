import { createClient, RedisClientType } from 'redis';
import { createHash } from 'crypto';
import { FastifyRequest, FastifyReply } from 'fastify';
import { config, CacheKey } from '../config.js';

let redisClient: RedisClientType | null = null;

export async function initRedis(): Promise<void> {
  try {
    redisClient = createClient({
      socket: {
        host: config.redis.host,
        port: config.redis.port,
      },
      database: config.redis.db,
    });

    redisClient.on('error', (err) => {
      console.error('Redis error:', err);
    });

    await redisClient.connect();
    await redisClient.ping();
    console.log('Redis connected successfully');
  } catch (err) {
    console.error('Redis connection failed:', err);
    redisClient = null;
  }
}

export function getRedisClient(): RedisClientType | null {
  return redisClient;
}

function generateCacheKey(cacheKeyPrefix: string, query: unknown): string {
  const queryString = JSON.stringify(query, Object.keys(query as object).sort());
  const hash = createHash('md5').update(queryString).digest('hex');
  return `${cacheKeyPrefix}:${hash}`;
}

export function createCacheHooks(cacheKeyPrefix: CacheKey) {
  const ttl = config.cacheTtl[cacheKeyPrefix];

  return {
    preHandler: async (request: FastifyRequest, reply: FastifyReply) => {
      if (!redisClient) return;

      const fullCacheKey = generateCacheKey(cacheKeyPrefix, request.query);

      try {
        const cached = await redisClient.get(fullCacheKey);
        if (cached) {
          console.log(`[CACHE HIT] ${cacheKeyPrefix}:${fullCacheKey.slice(-8)}`);
          reply.header('Content-Type', 'application/json');
          reply.send(cached);
          return reply;
        }
        console.log(`[CACHE MISS] ${cacheKeyPrefix}:${fullCacheKey.slice(-8)}`);
      } catch (err) {
        console.error('Redis get error:', err);
      }
    },

    onSend: async (
      request: FastifyRequest,
      reply: FastifyReply,
      payload: unknown
    ): Promise<unknown> => {
      if (!redisClient) return payload;

      // Only cache successful responses
      if (reply.statusCode !== 200) return payload;

      // Only cache string payloads (JSON responses)
      if (typeof payload !== 'string') return payload;

      const fullCacheKey = generateCacheKey(cacheKeyPrefix, request.query);

      try {
        await redisClient.setEx(fullCacheKey, ttl, payload);
        console.log(`[CACHE SET] ${cacheKeyPrefix}:${fullCacheKey.slice(-8)} (TTL: ${ttl}s)`);
      } catch (err) {
        console.error('Redis set error:', err);
      }

      return payload;
    },
  };
}
