import { FastifyInstance } from 'fastify';
import { createCacheHooks } from '../cache/redis.js';
import { getPlanStats } from '../services/planMetrics.js';
import type { PlanPeriod } from '../types/index.js';

const ALLOWED_PERIODS = ['6h', '24h', '7d', '30d', '90d', 'total'] as const;

interface PlanStatsQuery {
  period?: string;
}

const planStatsQuerySchema = {
  type: 'object',
  properties: {
    period: {
      type: 'string',
      enum: ALLOWED_PERIODS,
      default: '7d',
      description: 'Time period: 6h, 24h, 7d, 30d, 90d, or total',
    },
  },
};

export async function planStatsRoutes(fastify: FastifyInstance): Promise<void> {
  const cacheHooks = createCacheHooks('plan_stats');

  fastify.get<{ Querystring: PlanStatsQuery }>(
    '/metrics/plans',
    {
      schema: {
        querystring: planStatsQuerySchema,
        response: {
          200: {
            type: 'object',
            properties: {
              period: { type: 'string' },
              granularity: { type: 'string', enum: ['hourly', 'daily'] },
              data_cutoff: { type: 'string', format: 'date-time' },
              range: {
                type: 'object',
                properties: {
                  start: { type: 'string' },
                  end: { type: 'string' },
                },
              },
              totals: {
                type: 'object',
                properties: {
                  active_nodes: { type: 'number' },
                  total_fees: { type: 'number' },
                  compute_hours: { type: 'number' },
                  core_hours: { type: 'number' },
                  ram_hours: { type: 'number' },
                  gpu_hours: { type: 'number' },
                },
              },
              gpu_hours_by_model: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    group: { type: 'string' },
                    value: { type: 'number' },
                  },
                },
              },
              gpu_hours_by_vram: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    group: { type: 'string' },
                    value: { type: 'number' },
                  },
                },
              },
              active_nodes_by_gpu_model: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    group: { type: 'string' },
                    value: { type: 'number' },
                  },
                },
              },
              active_nodes_by_vram: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    group: { type: 'string' },
                    value: { type: 'number' },
                  },
                },
              },
              time_series: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    timestamp: { type: 'string', format: 'date-time' },
                    active_nodes: { type: 'number' },
                    total_fees: { type: 'number' },
                    compute_hours: { type: 'number' },
                    core_hours: { type: 'number' },
                    ram_hours: { type: 'number' },
                    gpu_hours: { type: 'number' },
                  },
                },
              },
              gpu_hours_by_model_ts: {
                type: 'object',
                properties: {
                  labels: { type: 'array', items: { type: 'string' } },
                  datasets: {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        label: { type: 'string' },
                        data: { type: 'array', items: { type: 'number' } },
                      },
                    },
                  },
                },
              },
              gpu_hours_by_vram_ts: {
                type: 'object',
                properties: {
                  labels: { type: 'array', items: { type: 'string' } },
                  datasets: {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        label: { type: 'string' },
                        data: { type: 'array', items: { type: 'number' } },
                      },
                    },
                  },
                },
              },
              active_nodes_by_gpu_model_ts: {
                type: 'object',
                properties: {
                  labels: { type: 'array', items: { type: 'string' } },
                  datasets: {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        label: { type: 'string' },
                        data: { type: 'array', items: { type: 'number' } },
                      },
                    },
                  },
                },
              },
              active_nodes_by_vram_ts: {
                type: 'object',
                properties: {
                  labels: { type: 'array', items: { type: 'string' } },
                  datasets: {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        label: { type: 'string' },
                        data: { type: 'array', items: { type: 'number' } },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      preHandler: cacheHooks.preHandler,
      onSend: cacheHooks.onSend,
    },
    async (request, reply) => {
      const period = (request.query.period || '7d') as PlanPeriod;

      if (!ALLOWED_PERIODS.includes(period as typeof ALLOWED_PERIODS[number])) {
        return reply.status(400).send({
          error: `Invalid period. Allowed: ${ALLOWED_PERIODS.join(', ')}`,
        });
      }

      const result = await getPlanStats(period);
      return result;
    }
  );
}
