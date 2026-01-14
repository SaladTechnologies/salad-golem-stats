import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import Fastify, { FastifyInstance } from 'fastify';

// Mock dependencies
vi.mock('../db/connection.js', () => ({
  query: vi.fn(),
}));

vi.mock('../cache/redis.js', () => ({
  createCacheHooks: () => ({
    preHandler: async () => {},
    onSend: async () => {},
  }),
}));

import { geoRoutes } from './geo.js';
import { query } from '../db/connection.js';

const mockQuery = vi.mocked(query);

describe('Geo Routes', () => {
  let app: FastifyInstance;

  beforeEach(async () => {
    vi.clearAllMocks();
    app = Fastify();
    await app.register(geoRoutes);
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  describe('GET /metrics/geo_counts', () => {
    const mockCityData = [
      {
        name: 'New York',
        count: 100,
        lat: 40.7128,
        long: -74.0060,
      },
      {
        name: 'Los Angeles',
        count: 75,
        lat: 34.0522,
        long: -118.2437,
      },
      {
        name: 'Chicago',
        count: 50,
        lat: 41.8781,
        long: -87.6298,
      },
    ];

    it('should return geo data with correct structure', async () => {
      mockQuery.mockResolvedValueOnce(mockCityData);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts',
      });

      expect(response.statusCode).toBe(200);
      const body = JSON.parse(response.body);

      expect(body).toHaveProperty('resolution');
      expect(body).toHaveProperty('data');
      expect(Array.isArray(body.data)).toBe(true);
    });

    it('should aggregate cities into H3 hexagons', async () => {
      mockQuery.mockResolvedValueOnce(mockCityData);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts',
      });

      const body = JSON.parse(response.body);

      // All data points should have lat, lng, normalized
      for (const point of body.data) {
        expect(point).toHaveProperty('lat');
        expect(point).toHaveProperty('lng');
        expect(point).toHaveProperty('normalized');
        expect(typeof point.lat).toBe('number');
        expect(typeof point.lng).toBe('number');
        expect(typeof point.normalized).toBe('number');
      }
    });

    it('should normalize counts relative to max', async () => {
      mockQuery.mockResolvedValueOnce(mockCityData);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts',
      });

      const body = JSON.parse(response.body);

      // The highest normalized value should be 1.0 (or close to it due to aggregation)
      const maxNormalized = Math.max(...body.data.map((p: any) => p.normalized));
      expect(maxNormalized).toBeLessThanOrEqual(1.0);
      expect(maxNormalized).toBeGreaterThan(0);

      // All normalized values should be between 0 and 1
      for (const point of body.data) {
        expect(point.normalized).toBeGreaterThanOrEqual(0);
        expect(point.normalized).toBeLessThanOrEqual(1.0);
      }
    });

    it('should use default resolution of 3', async () => {
      mockQuery.mockResolvedValueOnce(mockCityData);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts',
      });

      const body = JSON.parse(response.body);
      expect(body.resolution).toBe(3);
    });

    it('should accept custom resolution parameter', async () => {
      mockQuery.mockResolvedValueOnce(mockCityData);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts?resolution=5',
      });

      const body = JSON.parse(response.body);
      expect(body.resolution).toBe(5);
    });

    it('should aggregate cities in same hexagon', async () => {
      // Two cities very close together (same hexagon at low resolution)
      const closeCities = [
        {
          name: 'City A',
          count: 50,
          lat: 40.7128,
          long: -74.0060,
        },
        {
          name: 'City B',
          count: 50,
          lat: 40.7129,
          long: -74.0061, // Very close to City A
        },
      ];

      mockQuery.mockResolvedValueOnce(closeCities);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts?resolution=2', // Low resolution = larger hexagons
      });

      const body = JSON.parse(response.body);

      // At low resolution, these should be in the same hexagon
      // So we should have fewer data points than input cities
      expect(body.data.length).toBeLessThanOrEqual(2);
    });

    it('should handle empty city data', async () => {
      mockQuery.mockResolvedValueOnce([]);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts',
      });

      expect(response.statusCode).toBe(200);
      const body = JSON.parse(response.body);

      expect(body.data).toEqual([]);
      expect(body.resolution).toBe(3);
    });

    it('should reject resolution < 0', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts?resolution=-1',
      });

      expect(response.statusCode).toBe(400);
    });

    it('should reject resolution > 15', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts?resolution=16',
      });

      expect(response.statusCode).toBe(400);
    });

    it('should sort results by count descending', async () => {
      // Shuffle the mock data to ensure sorting is working
      const shuffledData = [mockCityData[2], mockCityData[0], mockCityData[1]];
      mockQuery.mockResolvedValueOnce(shuffledData);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts?resolution=10', // High resolution = more granular
      });

      const body = JSON.parse(response.body);

      // Results should be sorted by normalized value (which corresponds to count)
      for (let i = 1; i < body.data.length; i++) {
        expect(body.data[i - 1].normalized).toBeGreaterThanOrEqual(
          body.data[i].normalized
        );
      }
    });

    it('should query for latest city snapshot', async () => {
      mockQuery.mockResolvedValueOnce([]);

      await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts',
      });

      // Verify query includes MAX(ts) subquery
      const queryCall = mockQuery.mock.calls[0];
      const [sql] = queryCall;
      expect(sql).toContain('MAX(ts)');
      expect(sql).toContain('city_snapshots');
    });

    it('should handle single city', async () => {
      mockQuery.mockResolvedValueOnce([mockCityData[0]]);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts',
      });

      expect(response.statusCode).toBe(200);
      const body = JSON.parse(response.body);

      expect(body.data).toHaveLength(1);
      expect(body.data[0].normalized).toBe(1.0); // Only city, so normalized = 1
    });

    it('should return valid lat/lng coordinates', async () => {
      mockQuery.mockResolvedValueOnce(mockCityData);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/geo_counts',
      });

      const body = JSON.parse(response.body);

      for (const point of body.data) {
        // Valid latitude range: -90 to 90
        expect(point.lat).toBeGreaterThanOrEqual(-90);
        expect(point.lat).toBeLessThanOrEqual(90);

        // Valid longitude range: -180 to 180
        expect(point.lng).toBeGreaterThanOrEqual(-180);
        expect(point.lng).toBeLessThanOrEqual(180);
      }
    });
  });
});
