import { FastifyInstance } from 'fastify';
import { query, queryOne } from '../db/connection.js';
import { getQueryParameters } from '../services/queryParams.js';
import { createCacheHooks } from '../cache/redis.js';
import { Period, Metric } from '../types/index.js';

const ALLOWED_PERIODS = ['day', 'week', 'two_weeks', 'month'] as const;

interface StatsQuery {
  period?: string;
  gpu?: string;
}

const statsQuerySchema = {
  type: 'object',
  properties: {
    period: { type: 'string', enum: ALLOWED_PERIODS, default: 'week' },
    gpu: { type: 'string', default: 'all' },
  },
};

export async function statsRoutes(fastify: FastifyInstance): Promise<void> {
  const cacheHooks = createCacheHooks('stats');

  fastify.get<{ Querystring: StatsQuery }>(
    '/metrics/stats',
    {
      schema: { querystring: statsQuerySchema },
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
        'unique_node_count',
        'total_transaction_count',
      ];

      const results: Record<string, number> = {};

      for (const metric of metrics) {
        const { since, table, tsCol } = getQueryParameters(metric, period);

        let sql: string;
        if (table === 'hourly_gpu_stats' && tsCol === 'day') {
          sql = `
            SELECT SUM(${metric}) as value
            FROM ${table}
            WHERE gpu_group = $1 AND hour >= $2
          `;
        } else {
          sql = `
            SELECT SUM(${metric}) as value
            FROM ${table}
            WHERE gpu_group = $1 AND ${tsCol} >= $2
          `;
        }

        const row = await queryOne<{ value: number | null }>(sql, [gpu, since]);
        results[metric] = row?.value ?? 0;
      }

      return results;
    }
  );
}
