import { FastifyInstance } from 'fastify';
import { statsRoutes } from './stats.js';
import { trendsRoutes } from './trends.js';
import { geoRoutes } from './geo.js';
import { transactionsRoutes } from './transactions.js';
import { gpuStatsRoutes } from './gpuStats.js';

export async function registerRoutes(fastify: FastifyInstance): Promise<void> {
  await fastify.register(statsRoutes);
  await fastify.register(trendsRoutes);
  await fastify.register(geoRoutes);
  await fastify.register(transactionsRoutes);
  await fastify.register(gpuStatsRoutes);
}
