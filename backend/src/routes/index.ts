import { FastifyInstance } from 'fastify';
import { geoRoutes } from './geo.js';
import { transactionsRoutes } from './transactions.js';
import { planStatsRoutes } from './planStats.js';
import { golemStatsRoutes } from './golemStats.js';

export async function registerRoutes(fastify: FastifyInstance): Promise<void> {
  await fastify.register(geoRoutes);
  await fastify.register(transactionsRoutes);
  await fastify.register(planStatsRoutes);
  await fastify.register(golemStatsRoutes);
}
