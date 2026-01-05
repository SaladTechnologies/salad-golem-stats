import { FastifyInstance } from 'fastify';
import { getMetricsByGpu } from '../services/metrics.js';
import { createCacheHooks } from '../cache/redis.js';
import { Period, Metric, GpuStatsResponse } from '../types/index.js';

const ALLOWED_PERIODS = ['day', 'week', 'two_weeks', 'month'] as const;

interface GpuStatsQuery {
  period?: string;
  metric?: string;
}

const gpuStatsQuerySchema = {
  type: 'object',
  properties: {
    period: { type: 'string', enum: ALLOWED_PERIODS, default: 'week' },
    metric: { type: 'string', default: 'total_time_hours' },
  },
};

export async function gpuStatsRoutes(fastify: FastifyInstance): Promise<void> {
  const cacheHooks = createCacheHooks('gpu_stats');

  fastify.get<{ Querystring: GpuStatsQuery }>(
    '/metrics/gpu_stats',
    {
      schema: { querystring: gpuStatsQuerySchema },
      preHandler: cacheHooks.preHandler,
      onSend: cacheHooks.onSend,
    },
    async (request, reply) => {
      const period = (request.query.period || 'week') as Period;
      const metric = (request.query.metric || 'total_time_hours') as Metric;

      if (!ALLOWED_PERIODS.includes(period as typeof ALLOWED_PERIODS[number])) {
        return reply.status(400).send({ error: `Invalid period. Allowed: ${ALLOWED_PERIODS.join(', ')}` });
      }

      const result = await getMetricsByGpu(metric, period);

      return { [metric]: result } as GpuStatsResponse;
    }
  );
}
