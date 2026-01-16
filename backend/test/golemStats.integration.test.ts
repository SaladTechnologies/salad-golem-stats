import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from 'vitest';
import Fastify from 'fastify';
import type { FastifyInstance } from 'fastify';
import {
  setupTestDatabase,
  teardownTestDatabase,
  cleanupTestData,
  closeTestDatabase,
  insertTestNodePlans,
  insertTestGpuClasses,
  testPool,
} from './dbSetup.js';
import { testNodePlans, testGpuClasses } from './fixtures.js';

// Mock the database connection to use test pool
vi.mock('../src/db/connection.js', () => ({
  query: async <T>(sql: string, params: unknown[]): Promise<T[]> => {
    const result = await testPool.query(sql, params);
    return result.rows as T[];
  },
}));

// Mock config with test values
vi.mock('../src/config.js', () => ({
  config: {
    golemApiToken: 'test-secret-token-12345',
    golemUtilizationGranularitySeconds: 30,
    cacheTtl: {
      geo: 86400,
      transactions: 60,
      plan_stats: 3600,
      golem_stats: 300,
      golem_historical: 600,
    },
  },
}));

// Now import the routes after mocks are set up
const { golemStatsRoutes } = await import('../src/routes/golemStats.js');

describe('Golem Stats Integration Tests', () => {
  let app: FastifyInstance;
  const testApiToken = 'test-secret-token-12345';

  beforeAll(async () => {
    // Setup test database
    await setupTestDatabase();
  });

  afterAll(async () => {
    // Cleanup
    await teardownTestDatabase();
    await closeTestDatabase();
  });

  beforeEach(async () => {
    // Clean test data and insert fresh fixtures
    await cleanupTestData();
    await insertTestGpuClasses(testGpuClasses);
    await insertTestNodePlans(testNodePlans);

    // Create fresh Fastify instance for each test
    app = Fastify();
    await app.register(golemStatsRoutes);
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  describe('Authentication', () => {
    it('should reject requests without Authorization header', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats',
      });

      expect(response.statusCode).toBe(401);
      const body = JSON.parse(response.body);
      expect(body.error).toBe('Unauthorized');
    });

    it('should reject requests with invalid Bearer token', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats',
        headers: {
          authorization: 'Bearer wrong-token',
        },
      });

      expect(response.statusCode).toBe(403);
      const body = JSON.parse(response.body);
      expect(body.error).toBe('Forbidden');
    });

    it('should reject requests without Bearer prefix', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats',
        headers: {
          authorization: testApiToken,
        },
      });

      expect(response.statusCode).toBe(401);
    });

    it('should accept requests with valid Bearer token', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      expect(response.statusCode).toBe(200);
    });
  });

  describe('GET /v1/network/stats', () => {
    it('should return current network statistics with correct structure', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      expect(response.statusCode).toBe(200);
      const body = JSON.parse(response.body);

      // Validate structure
      expect(body).toHaveProperty('timestamp');
      expect(body).toHaveProperty('network_id', 'salad');
      expect(body).toHaveProperty('providers');
      expect(body).toHaveProperty('resources');
      expect(body).toHaveProperty('earnings');
      expect(body).toHaveProperty('versions');

      // Validate providers
      expect(body.providers).toHaveProperty('online');
      expect(body.providers).toHaveProperty('computing');
      expect(typeof body.providers.online).toBe('number');
      expect(typeof body.providers.computing).toBe('number');

      // Validate resources
      expect(body.resources).toHaveProperty('cores');
      expect(body.resources).toHaveProperty('memory_gib');
      expect(body.resources).toHaveProperty('disk_gib');
      expect(body.resources).toHaveProperty('gpus');
      expect(typeof body.resources.cores).toBe('number');
      expect(typeof body.resources.memory_gib).toBe('number');
      expect(body.resources.disk_gib).toBe(0); // Should always be 0

      // Validate earnings
      expect(body.earnings).toHaveProperty('6h');
      expect(body.earnings).toHaveProperty('24h');
      expect(body.earnings).toHaveProperty('168h');
      expect(body.earnings).toHaveProperty('720h');
      expect(body.earnings).toHaveProperty('2160h');
      expect(body.earnings).toHaveProperty('total');

      // Validate versions
      expect(Array.isArray(body.versions)).toBe(true);
    });

    it('should have computing nodes equal to online nodes', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      const body = JSON.parse(response.body);
      expect(body.providers.computing).toBe(body.providers.online);
    });

    it('should return non-zero resource values with test data', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      const body = JSON.parse(response.body);

      // With test fixtures, we should have resources
      expect(body.resources.cores).toBeGreaterThan(0);
      expect(body.resources.memory_gib).toBeGreaterThan(0);
      // disk_gib should always be 0
      expect(body.resources.disk_gib).toBe(0);
    });

    it('should return valid timestamp', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      const body = JSON.parse(response.body);
      const timestamp = new Date(body.timestamp);
      expect(timestamp.getTime()).toBeGreaterThan(Date.now() - 60000); // Within last minute
      expect(timestamp.getTime()).toBeLessThanOrEqual(Date.now());
    });
  });

  describe('GET /v1/network/stats/historical', () => {
    it('should return historical statistics with correct structure', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats/historical',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      expect(response.statusCode).toBe(200);
      const body = JSON.parse(response.body);

      // Validate structure
      expect(body).toHaveProperty('network_id', 'salad');
      expect(body).toHaveProperty('network_stats');
      expect(body).toHaveProperty('utilization');
      expect(body).toHaveProperty('computing_daily');

      // Validate network_stats
      expect(body.network_stats).toHaveProperty('vm');
      expect(body.network_stats).toHaveProperty('vm-nvidia');
      expect(Array.isArray(body.network_stats.vm)).toBe(true);
      expect(Array.isArray(body.network_stats['vm-nvidia'])).toBe(true);
    });

    it('should return utilization data with correct format', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats/historical',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      const body = JSON.parse(response.body);

      // Utilization should be array of [timestamp, count] tuples
      expect(Array.isArray(body.utilization)).toBe(true);

      if (body.utilization.length > 0) {
        const firstPoint = body.utilization[0];
        expect(Array.isArray(firstPoint)).toBe(true);
        expect(firstPoint.length).toBe(2);
        expect(typeof firstPoint[0]).toBe('number'); // timestamp
        expect(typeof firstPoint[1]).toBe('number'); // count
      }
    });

    it('should return network stats with correct data point structure', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats/historical',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      const body = JSON.parse(response.body);

      // Check VM data points if any exist
      if (body.network_stats.vm.length > 0) {
        const dataPoint = body.network_stats.vm[0];
        expect(dataPoint).toHaveProperty('date');
        expect(dataPoint).toHaveProperty('online');
        expect(dataPoint).toHaveProperty('cores');
        expect(dataPoint).toHaveProperty('memory_gib');
        expect(dataPoint).toHaveProperty('disk_gib');
        expect(dataPoint).toHaveProperty('gpus');
        expect(dataPoint.disk_gib).toBe(0); // Should always be 0
      }

      // Check VM-NVIDIA data points if any exist
      if (body.network_stats['vm-nvidia'].length > 0) {
        const dataPoint = body.network_stats['vm-nvidia'][0];
        expect(dataPoint).toHaveProperty('date');
        expect(dataPoint).toHaveProperty('online');
        expect(dataPoint.disk_gib).toBe(0); // Should always be 0
      }
    });

    it('should return computing_daily data with correct structure', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats/historical',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      const body = JSON.parse(response.body);

      expect(Array.isArray(body.computing_daily)).toBe(true);

      if (body.computing_daily.length > 0) {
        const dailyPoint = body.computing_daily[0];
        expect(dailyPoint).toHaveProperty('date');
        expect(dailyPoint).toHaveProperty('total');
        expect(typeof dailyPoint.date).toBe('string');
        expect(typeof dailyPoint.total).toBe('number');
      }
    });

    it('should have 720 utilization data points (6 hours at 30s intervals)', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats/historical',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      const body = JSON.parse(response.body);

      // 6 hours * 60 minutes * 60 seconds / 30 seconds = 720 intervals
      const expectedPoints = (6 * 60 * 60) / 30;
      expect(body.utilization.length).toBe(expectedPoints);
    });

    it('should have utilization timestamps in chronological order', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats/historical',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      const body = JSON.parse(response.body);

      for (let i = 1; i < body.utilization.length; i++) {
        expect(body.utilization[i][0]).toBeGreaterThan(body.utilization[i - 1][0]);
      }
    });

    it('should have 30-second intervals between utilization data points', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats/historical',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      const body = JSON.parse(response.body);

      if (body.utilization.length > 1) {
        const interval = body.utilization[1][0] - body.utilization[0][0];
        expect(interval).toBe(30); // 30 seconds
      }
    });
  });

  describe('Response Headers', () => {
    it('should return JSON content type for stats endpoint', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      expect(response.headers['content-type']).toContain('application/json');
    });

    it('should return JSON content type for historical endpoint', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/v1/network/stats/historical',
        headers: {
          authorization: `Bearer ${testApiToken}`,
        },
      });

      expect(response.headers['content-type']).toContain('application/json');
    });
  });
});
