import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { timingSafeEqual } from 'crypto';
import { config } from '../config.js';
import { createCacheHooks } from '../cache/redis.js';
import { getGolemNetworkStats, getGolemHistoricalStats } from '../services/golemMetrics.js';

// Authentication middleware for Bearer token
async function authenticateGolem(request: FastifyRequest, reply: FastifyReply) {
  const authHeader = request.headers.authorization;

  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return reply.status(401).send({
      error: 'Unauthorized',
      message: 'Authorization header with Bearer token required',
    });
  }

  const token = authHeader.substring(7); // Remove 'Bearer ' prefix

  if (!config.golemApiToken) {
    return reply.status(500).send({
      error: 'Configuration error',
      message: 'GOLEM_API_TOKEN not configured',
    });
  }

  // Use timing-safe comparison to prevent timing attacks
  try {
    const tokenBuffer = Buffer.from(token);
    const configTokenBuffer = Buffer.from(config.golemApiToken);

    // If lengths differ, timingSafeEqual will throw - catch and return 403
    if (tokenBuffer.length !== configTokenBuffer.length) {
      return reply.status(403).send({
        error: 'Forbidden',
        message: 'Invalid API token',
      });
    }

    if (!timingSafeEqual(tokenBuffer, configTokenBuffer)) {
      return reply.status(403).send({
        error: 'Forbidden',
        message: 'Invalid API token',
      });
    }
  } catch (err) {
    return reply.status(403).send({
      error: 'Forbidden',
      message: 'Invalid API token',
    });
  }
}

export async function golemStatsRoutes(fastify: FastifyInstance): Promise<void> {
  const statsCacheHooks = createCacheHooks('golem_stats');
  const historicalCacheHooks = createCacheHooks('golem_historical');

  // Current snapshot endpoint
  fastify.get(
    '/v1/network/stats',
    {
      preHandler: [authenticateGolem, statsCacheHooks.preHandler],
      onSend: statsCacheHooks.onSend,
      schema: {
        response: {
          200: {
            type: 'object',
            properties: {
              timestamp: { type: 'string', format: 'date-time' },
              network_id: { type: 'string' },
              providers: {
                type: 'object',
                properties: {
                  online: { type: 'number' },
                  computing: { type: 'number' },
                },
              },
              resources: {
                type: 'object',
                properties: {
                  cores: { type: 'number' },
                  memory_gib: { type: 'number' },
                  disk_gib: { type: 'number' },
                  gpus: { type: 'number' },
                },
              },
              earnings: {
                type: 'object',
                properties: {
                  '6h': { type: 'number' },
                  '24h': { type: 'number' },
                  '168h': { type: 'number' },
                  '720h': { type: 'number' },
                  '2160h': { type: 'number' },
                  total: { type: 'number' },
                },
              },
              versions: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    version: { type: 'string' },
                    count: { type: 'number' },
                    rc: { type: 'boolean' },
                  },
                },
              },
            },
          },
        },
      },
    },
    async (request, reply) => {
      try {
        const result = await getGolemNetworkStats();
        return result;
      } catch (err) {
        request.log.error('Error generating Golem network stats:', err);
        return reply.status(500).send({
          error: 'Internal Server Error',
          message: 'Failed to retrieve network statistics',
        });
      }
    }
  );

  // Historical data endpoint
  fastify.get(
    '/v1/network/stats/historical',
    {
      preHandler: [authenticateGolem, historicalCacheHooks.preHandler],
      onSend: historicalCacheHooks.onSend,
      schema: {
        response: {
          200: {
            type: 'object',
            properties: {
              network_id: { type: 'string' },
              network_stats: {
                type: 'object',
                properties: {
                  vm: {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        date: { type: 'number' },
                        online: { type: 'number' },
                        cores: { type: 'number' },
                        memory_gib: { type: 'number' },
                        disk_gib: { type: 'number' },
                        gpus: { type: 'number' },
                      },
                    },
                  },
                  'vm-nvidia': {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        date: { type: 'number' },
                        online: { type: 'number' },
                        cores: { type: 'number' },
                        memory_gib: { type: 'number' },
                        disk_gib: { type: 'number' },
                        gpus: { type: 'number' },
                      },
                    },
                  },
                },
              },
              utilization: {
                type: 'array',
                items: {
                  type: 'array',
                  items: { type: 'number' },
                },
              },
              computing_daily: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    date: { type: 'string' },
                    total: { type: 'number' },
                  },
                },
              },
            },
          },
        },
      },
    },
    async (request, reply) => {
      try {
        const result = await getGolemHistoricalStats();
        return result;
      } catch (err) {
        request.log.error('Error generating Golem historical stats:', err);
        return reply.status(500).send({
          error: 'Internal Server Error',
          message: 'Failed to retrieve historical statistics',
        });
      }
    }
  );
}
