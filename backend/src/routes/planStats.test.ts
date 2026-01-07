import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import Fastify from 'fastify';
import type { FastifyInstance } from 'fastify';
import { planStatsRoutes } from './planStats.js';

// Mock the planMetrics service
vi.mock('../services/planMetrics.js', () => ({
  getPlanStats: vi.fn(),
}));

// Mock the cache hooks to be pass-through
vi.mock('../cache/redis.js', () => ({
  createCacheHooks: () => ({
    preHandler: async () => {},
    onSend: async (_req: unknown, _reply: unknown, payload: unknown) => payload,
  }),
}));

import { getPlanStats } from '../services/planMetrics.js';

const mockGetPlanStats = vi.mocked(getPlanStats);

describe('planStats routes', () => {
  let app: FastifyInstance;

  const mockResponse = {
    period: '7d' as const,
    granularity: 'hourly' as const,
    data_cutoff: '2025-12-23T12:00:00.000Z',
    range: {
      start: '2025-12-16T12:00:00.000Z',
      end: '2025-12-23T12:00:00.000Z',
    },
    totals: {
      active_nodes: 100,
      total_fees: 5000.5,
      compute_hours: 1000.25,
      transactions: 500,
      core_hours: 2000.75,
      ram_hours: 3000.5,
      gpu_hours: 800.25,
    },
    gpu_hours_by_model: [
      { group: 'RTX 4090 (24 GB)', value: 500.5 },
    ],
    gpu_hours_by_vram: [
      { group: '24 GB', value: 800.25 },
    ],
    active_nodes_by_gpu_model: [
      { group: 'RTX 4090 (24 GB)', value: 60 },
    ],
    active_nodes_by_vram: [
      { group: '24 GB', value: 60 },
    ],
    time_series: [
      {
        timestamp: '2025-12-23T00:00:00.000Z',
        active_nodes: 50,
        total_fees: 2500.25,
        compute_hours: 500.125,
        transactions: 250,
        core_hours: 1000.375,
        ram_hours: 1500.25,
        gpu_hours: 400.125,
      },
    ],
    gpu_hours_by_model_ts: {
      labels: ['2025-12-23T00:00:00.000Z'],
      datasets: [{ label: 'RTX 4090 (24 GB)', data: [400.125] }],
    },
    gpu_hours_by_vram_ts: {
      labels: ['2025-12-23T00:00:00.000Z'],
      datasets: [{ label: '24 GB', data: [400.125] }],
    },
    active_nodes_by_gpu_model_ts: {
      labels: ['2025-12-23T00:00:00.000Z'],
      datasets: [{ label: 'RTX 4090 (24 GB)', data: [50] }],
    },
    active_nodes_by_vram_ts: {
      labels: ['2025-12-23T00:00:00.000Z'],
      datasets: [{ label: '24 GB', data: [50] }],
    },
  };

  beforeEach(async () => {
    vi.clearAllMocks();
    app = Fastify();
    await app.register(planStatsRoutes);
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  describe('GET /metrics/plans', () => {
    it('should return 200 with valid response', async () => {
      mockGetPlanStats.mockResolvedValue(mockResponse);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/plans',
      });

      expect(response.statusCode).toBe(200);
      const body = JSON.parse(response.body);
      expect(body).toEqual(mockResponse);
    });

    it('should use default period of 7d when not specified', async () => {
      mockGetPlanStats.mockResolvedValue(mockResponse);

      await app.inject({
        method: 'GET',
        url: '/metrics/plans',
      });

      expect(mockGetPlanStats).toHaveBeenCalledWith('7d');
    });

    it('should accept valid period query params', async () => {
      const validPeriods = ['6h', '24h', '7d', '30d', '90d', 'total'];

      for (const period of validPeriods) {
        mockGetPlanStats.mockResolvedValue({ ...mockResponse, period: period as typeof mockResponse.period });

        const response = await app.inject({
          method: 'GET',
          url: `/metrics/plans?period=${period}`,
        });

        expect(response.statusCode).toBe(200);
        expect(mockGetPlanStats).toHaveBeenCalledWith(period);
      }
    });

    it('should reject invalid period with 400', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/metrics/plans?period=invalid',
      });

      expect(response.statusCode).toBe(400);
    });

    it('should return correct content-type', async () => {
      mockGetPlanStats.mockResolvedValue(mockResponse);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/plans',
      });

      expect(response.headers['content-type']).toContain('application/json');
    });

    it('should handle service errors gracefully', async () => {
      mockGetPlanStats.mockRejectedValue(new Error('Database error'));

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/plans',
      });

      expect(response.statusCode).toBe(500);
    });

    it('should return totals in correct format', async () => {
      mockGetPlanStats.mockResolvedValue(mockResponse);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/plans',
      });

      const body = JSON.parse(response.body);
      expect(body.totals).toHaveProperty('active_nodes');
      expect(body.totals).toHaveProperty('total_fees');
      expect(body.totals).toHaveProperty('compute_hours');
      expect(body.totals).toHaveProperty('transactions');
      expect(body.totals).toHaveProperty('core_hours');
      expect(body.totals).toHaveProperty('ram_hours');
      expect(body.totals).toHaveProperty('gpu_hours');
      expect(typeof body.totals.active_nodes).toBe('number');
    });

    it('should return time series array', async () => {
      mockGetPlanStats.mockResolvedValue(mockResponse);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/plans',
      });

      const body = JSON.parse(response.body);
      expect(Array.isArray(body.time_series)).toBe(true);
      expect(body.time_series.length).toBeGreaterThan(0);
      expect(body.time_series[0]).toHaveProperty('timestamp');
      expect(body.time_series[0]).toHaveProperty('active_nodes');
    });

    it('should return GPU breakdowns', async () => {
      mockGetPlanStats.mockResolvedValue(mockResponse);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/plans',
      });

      const body = JSON.parse(response.body);
      expect(Array.isArray(body.gpu_hours_by_model)).toBe(true);
      expect(Array.isArray(body.gpu_hours_by_vram)).toBe(true);
      expect(Array.isArray(body.active_nodes_by_gpu_model)).toBe(true);
      expect(Array.isArray(body.active_nodes_by_vram)).toBe(true);

      if (body.gpu_hours_by_model.length > 0) {
        expect(body.gpu_hours_by_model[0]).toHaveProperty('group');
        expect(body.gpu_hours_by_model[0]).toHaveProperty('value');
      }
    });
  });
});
