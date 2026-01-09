import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { PlanPeriod } from '../types/index.js';

// Mock the database module before importing the service
vi.mock('../db/connection.js', () => ({
  query: vi.fn(),
}));

import { getPlanStats } from './planMetrics.js';
import { query } from '../db/connection.js';

const mockQuery = vi.mocked(query);

describe('planMetrics service', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Freeze time for consistent testing
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2025-12-25T12:00:00Z'));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('getPlanStats', () => {
    const mockTotalsRow = {
      active_nodes: '100',
      total_fees: '5000.50',
      compute_hours: '1000.25',
      transactions: '500',
      core_hours: '2000.75',
      ram_hours: '3000.50',
      gpu_hours: '800.25',
    };

    const mockTimeSeriesRows = [
      {
        bucket: new Date('2025-12-23T00:00:00Z'),
        active_nodes: '50',
        total_fees: '2500.25',
        compute_hours: '500.125',
        transactions: '250',
        core_hours: '1000.375',
        ram_hours: '1500.25',
        gpu_hours: '400.125',
      },
      {
        bucket: new Date('2025-12-24T00:00:00Z'),
        active_nodes: '50',
        total_fees: '2500.25',
        compute_hours: '500.125',
        transactions: '250',
        core_hours: '1000.375',
        ram_hours: '1500.25',
        gpu_hours: '400.125',
      },
    ];

    const mockGpuByModelRows = [
      { group_name: 'RTX 4090 (24 GB)', value: '500.5' },
      { group_name: 'RTX 3090 (24 GB)', value: '299.75' },
    ];

    const mockGpuByVramRows = [
      { group_name: '24 GB', value: '800.25' },
    ];

    const mockNodesByModelRows = [
      { group_name: 'RTX 4090 (24 GB)', value: '60' },
      { group_name: 'No GPU', value: '40' },
    ];

    const mockNodesByVramRows = [
      { group_name: 'No GPU', value: '40' },
      { group_name: '24 GB', value: '60' },
    ];

    beforeEach(() => {
      // Set up mock responses for each query in order
      mockQuery
        .mockResolvedValueOnce([mockTotalsRow]) // totals query
        .mockResolvedValueOnce(mockTimeSeriesRows) // time series query
        .mockResolvedValueOnce(mockGpuByModelRows) // gpu hours by model
        .mockResolvedValueOnce(mockGpuByVramRows) // gpu hours by vram
        .mockResolvedValueOnce(mockNodesByModelRows) // active nodes by model
        .mockResolvedValueOnce(mockNodesByVramRows); // active nodes by vram
    });

    it('should return correct structure for 7d period', async () => {
      const result = await getPlanStats('7d');

      expect(result).toHaveProperty('period', '7d');
      expect(result).toHaveProperty('granularity', 'hourly');
      expect(result).toHaveProperty('data_cutoff');
      expect(result).toHaveProperty('range');
      expect(result).toHaveProperty('totals');
      expect(result).toHaveProperty('gpu_hours_by_model');
      expect(result).toHaveProperty('gpu_hours_by_vram');
      expect(result).toHaveProperty('active_nodes_by_gpu_model');
      expect(result).toHaveProperty('active_nodes_by_vram');
      expect(result).toHaveProperty('time_series');
    });

    it('should use hourly granularity for periods <= 7d', async () => {
      const periods: PlanPeriod[] = ['6h', '24h', '7d'];

      for (const period of periods) {
        mockQuery
          .mockResolvedValueOnce([mockTotalsRow])
          .mockResolvedValueOnce(mockTimeSeriesRows)
          .mockResolvedValueOnce(mockGpuByModelRows)
          .mockResolvedValueOnce(mockGpuByVramRows)
          .mockResolvedValueOnce(mockNodesByModelRows)
          .mockResolvedValueOnce(mockNodesByVramRows);

        const result = await getPlanStats(period);
        expect(result.granularity).toBe('hourly');
      }
    });

    it('should use daily granularity for periods > 7d', async () => {
      const periods: PlanPeriod[] = ['30d', '90d', 'total'];

      for (const period of periods) {
        mockQuery
          .mockResolvedValueOnce([mockTotalsRow])
          .mockResolvedValueOnce(mockTimeSeriesRows)
          .mockResolvedValueOnce(mockGpuByModelRows)
          .mockResolvedValueOnce(mockGpuByVramRows)
          .mockResolvedValueOnce(mockNodesByModelRows)
          .mockResolvedValueOnce(mockNodesByVramRows);

        const result = await getPlanStats(period);
        expect(result.granularity).toBe('daily');
      }
    });

    it('should apply 48-hour data offset', async () => {
      const result = await getPlanStats('7d');

      // Current time is 2025-12-25T12:00:00Z
      // Cutoff should be 2025-12-23T12:00:00Z (48 hours before)
      const cutoffDate = new Date(result.data_cutoff);
      expect(cutoffDate.toISOString()).toBe('2025-12-23T12:00:00.000Z');
    });

    it('should calculate correct range for 7d period', async () => {
      const result = await getPlanStats('7d');

      // Cutoff: 2025-12-23T12:00:00Z
      // Start: 2025-12-16T12:00:00Z (7 days = 168 hours before cutoff)
      const startDate = new Date(result.range.start);
      const endDate = new Date(result.range.end);

      expect(startDate.toISOString()).toBe('2025-12-16T12:00:00.000Z');
      expect(endDate.toISOString()).toBe('2025-12-23T12:00:00.000Z');
    });

    it('should return "beginning" as start for total period', async () => {
      mockQuery
        .mockResolvedValueOnce([mockTotalsRow])
        .mockResolvedValueOnce(mockTimeSeriesRows)
        .mockResolvedValueOnce(mockGpuByModelRows)
        .mockResolvedValueOnce(mockGpuByVramRows)
        .mockResolvedValueOnce(mockNodesByModelRows)
        .mockResolvedValueOnce(mockNodesByVramRows);

      const result = await getPlanStats('total');

      expect(result.range.start).toBe('beginning');
    });

    it('should parse totals correctly', async () => {
      const result = await getPlanStats('7d');

      expect(result.totals).toEqual({
        active_nodes: 100,
        total_fees: 5000.5,
        compute_hours: 1000.25,
        transactions: 500,
        core_hours: 2000.75,
        ram_hours: 3000.5,
        gpu_hours: 800.25,
      });
    });

    it('should parse GPU breakdowns correctly', async () => {
      const result = await getPlanStats('7d');

      expect(result.gpu_hours_by_model).toEqual([
        { group: 'RTX 4090 (24 GB)', value: 500.5 },
        { group: 'RTX 3090 (24 GB)', value: 299.75 },
      ]);

      expect(result.gpu_hours_by_vram).toEqual([
        { group: '24 GB', value: 800.25 },
      ]);
    });

    it('should parse time series correctly', async () => {
      const result = await getPlanStats('7d');

      expect(result.time_series).toHaveLength(2);
      expect(result.time_series[0]).toEqual({
        timestamp: '2025-12-23T00:00:00.000Z',
        active_nodes: 50,
        total_fees: 2500.25,
        compute_hours: 500.125,
        transactions: 250,
        core_hours: 1000.375,
        ram_hours: 1500.25,
        gpu_hours: 400.125,
      });
    });

    it('should handle null values in database results', async () => {
      // Clear all previous mocks first
      mockQuery.mockReset();

      mockQuery
        .mockResolvedValueOnce([{
          active_nodes: '0',
          total_fees: null,
          compute_hours: null,
          transactions: '0',
          core_hours: null,
          ram_hours: null,
          gpu_hours: null,
        }])
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([]);

      const result = await getPlanStats('7d');

      expect(result.totals).toEqual({
        active_nodes: 0,
        total_fees: 0,
        compute_hours: 0,
        transactions: 0,
        core_hours: 0,
        ram_hours: 0,
        gpu_hours: 0,
      });
      expect(result.time_series).toEqual([]);
    });
  });

  describe('period to hours mapping', () => {
    const emptyTotalsRow = {
      active_nodes: '0',
      total_fees: '0',
      compute_hours: '0',
      transactions: '0',
      core_hours: '0',
      ram_hours: '0',
      gpu_hours: '0',
    };

    it('should query with correct time ranges for 6h period', async () => {
      mockQuery.mockReset();

      mockQuery
        .mockResolvedValueOnce([emptyTotalsRow]) // totals
        .mockResolvedValueOnce([]) // time series
        .mockResolvedValueOnce([]) // gpu by model
        .mockResolvedValueOnce([]) // gpu by vram
        .mockResolvedValueOnce([]) // nodes by model
        .mockResolvedValueOnce([]); // nodes by vram

      // Test 6h period
      await getPlanStats('6h');

      // The first call should be the totals query
      const totalsCall = mockQuery.mock.calls[0];
      const [, params] = totalsCall;

      // Should have 2 params: cutoff and start
      expect(params).toBeDefined();
      expect(params).toHaveLength(2);

      // Verify the difference is 6 hours (6 * 60 * 60 * 1000 ms)
      const cutoffMs = params![0] as number;
      const startMs = params![1] as number;
      const sixHoursMs = 6 * 60 * 60 * 1000;

      expect(cutoffMs - startMs).toBe(sixHoursMs);
    });

    it('should use only cutoff param for total period', async () => {
      mockQuery.mockReset();

      mockQuery
        .mockResolvedValueOnce([emptyTotalsRow]) // totals
        .mockResolvedValueOnce([]) // time series
        .mockResolvedValueOnce([]) // gpu by model
        .mockResolvedValueOnce([]) // gpu by vram
        .mockResolvedValueOnce([]) // nodes by model
        .mockResolvedValueOnce([]); // nodes by vram

      await getPlanStats('total');

      const totalsCall = mockQuery.mock.calls[0];
      const [, params] = totalsCall;

      // Total period should only have cutoff param (no start)
      expect(params).toHaveLength(1);
    });
  });
});
