import { FastifyInstance } from 'fastify';
import { getMetrics, getMetricsByGpu } from '../services/metrics.js';
import { createCacheHooks } from '../cache/redis.js';
import { Period, Metric } from '../types/index.js';

const ALLOWED_PERIODS = ['day', 'week', 'two_weeks', 'month'] as const;

interface TrendsQuery {
  period?: string;
  gpu?: string;
}

const trendsQuerySchema = {
  type: 'object',
  properties: {
    period: { type: 'string', enum: ALLOWED_PERIODS, default: 'week' },
    gpu: { type: 'string', default: 'all' },
  },
};

export async function trendsRoutes(fastify: FastifyInstance): Promise<void> {
  const cacheHooks = createCacheHooks('trends');

  fastify.get<{ Querystring: TrendsQuery }>(
    '/metrics/trends',
    {
      schema: { querystring: trendsQuerySchema },
      preHandler: cacheHooks.preHandler,
      onSend: cacheHooks.onSend,
    },
    async (request, reply) => {
      const period = (request.query.period || 'week') as Period;
      const gpu = request.query.gpu || 'all';

      if (!ALLOWED_PERIODS.includes(period as typeof ALLOWED_PERIODS[number])) {
        return reply.status(400).send({ error: `Invalid period. Allowed: ${ALLOWED_PERIODS.join(', ')}` });
      }

      const metrics: Metric[] = [
        'total_time_hours',
        'total_invoice_amount',
        'total_ram_hours',
        'total_cpu_hours',
        'total_transaction_count',
        'unique_node_count',
      ];

      const gpuMetrics: Metric[] = ['unique_node_count', 'total_time_hours'];
      const vramMetrics: Metric[] = ['unique_node_count', 'total_time_hours'];

      const result: Record<string, unknown> = {};

      // Fetch time series metrics
      for (const metric of metrics) {
        result[metric] = await getMetrics(metric, period, gpu);
      }

      // Fetch GPU breakdown metrics
      for (const metric of gpuMetrics) {
        result[`gpu_${metric}`] = await getMetricsByGpu(metric, period, 'gpu');
      }

      // Fetch VRAM breakdown metrics
      for (const metric of vramMetrics) {
        result[`vram_${metric}`] = await getMetricsByGpu(metric, period, 'vram');
      }

      return result;
    }
  );
}
