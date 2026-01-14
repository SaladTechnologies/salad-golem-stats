import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import Fastify, { FastifyInstance } from 'fastify';

// Mock dependencies
vi.mock('../db/connection.js', () => ({
  query: vi.fn(),
  queryOne: vi.fn(),
}));

vi.mock('../cache/redis.js', () => ({
  createCacheHooks: () => ({
    preHandler: async () => {},
    onSend: async () => {},
  }),
}));

vi.mock('../config.js', () => ({
  config: {
    transactionsMinDate: new Date('2024-01-01T00:00:00Z'),
  },
}));

import { transactionsRoutes } from './transactions.js';
import { query, queryOne } from '../db/connection.js';

const mockQuery = vi.mocked(query);
const mockQueryOne = vi.mocked(queryOne);

describe('Transactions Routes', () => {
  let app: FastifyInstance;

  beforeEach(async () => {
    vi.clearAllMocks();
    app = Fastify();
    await app.register(transactionsRoutes);
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  describe('GET /metrics/transactions', () => {
    const mockTransactions = [
      {
        tx_hash: '0xabc123',
        block_number: 12345,
        block_timestamp: new Date('2025-01-14T12:00:00Z'),
        from_address: '0xfrom1',
        to_address: '0xto1',
        value_glm: '100.5',
        tx_type: 'requester_to_provider',
      },
      {
        tx_hash: '0xdef456',
        block_number: 12344,
        block_timestamp: new Date('2025-01-14T11:00:00Z'),
        from_address: '0xfrom2',
        to_address: '0xto2',
        value_glm: '50.25',
        tx_type: 'requester_to_provider',
      },
    ];

    it('should return transactions with default parameters', async () => {
      mockQueryOne.mockResolvedValueOnce({ count: '2' }); // total count
      mockQuery.mockResolvedValueOnce(mockTransactions); // transactions
      mockQueryOne.mockResolvedValueOnce({ count: '0' }); // older count
      mockQueryOne.mockResolvedValueOnce({ count: '0' }); // newer count

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions',
      });

      expect(response.statusCode).toBe(200);
      const body = JSON.parse(response.body);

      expect(body).toHaveProperty('transactions');
      expect(body).toHaveProperty('next_cursor');
      expect(body).toHaveProperty('prev_cursor');
      expect(body).toHaveProperty('total', 2);
      expect(body.transactions).toHaveLength(2);
    });

    it('should parse transaction data correctly', async () => {
      mockQueryOne.mockResolvedValueOnce({ count: '1' });
      mockQuery.mockResolvedValueOnce([mockTransactions[0]]);
      mockQueryOne.mockResolvedValueOnce({ count: '0' });
      mockQueryOne.mockResolvedValueOnce({ count: '0' });

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions',
      });

      const body = JSON.parse(response.body);
      const tx = body.transactions[0];

      expect(tx.tx_hash).toBe('0xabc123');
      expect(tx.block_number).toBe(12345);
      expect(tx.block_timestamp).toBe('2025-01-14T12:00:00.000Z');
      expect(tx.from_address).toBe('0xfrom1');
      expect(tx.to_address).toBe('0xto1');
      expect(tx.value_glm).toBe(100.5);
      expect(tx.tx_type).toBe('requester_to_provider');
    });

    it('should respect limit parameter', async () => {
      mockQueryOne.mockResolvedValueOnce({ count: '100' });
      mockQuery.mockResolvedValueOnce(mockTransactions);
      mockQueryOne.mockResolvedValueOnce({ count: '0' });
      mockQueryOne.mockResolvedValueOnce({ count: '0' });

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions?limit=5',
      });

      expect(response.statusCode).toBe(200);

      // Check that the query was called with limit 5
      const queryCall = mockQuery.mock.calls[0];
      const params = queryCall[1] as unknown[];
      expect(params[params.length - 1]).toBe(5);
    });

    it('should use cursor for pagination', async () => {
      const cursor = '2025-01-14T11:00:00.000Z';

      mockQueryOne.mockResolvedValueOnce({ count: '2' });
      mockQuery.mockResolvedValueOnce([mockTransactions[1]]);
      mockQueryOne.mockResolvedValueOnce({ count: '0' });
      mockQueryOne.mockResolvedValueOnce({ count: '1' });

      const response = await app.inject({
        method: 'GET',
        url: `/metrics/transactions?cursor=${encodeURIComponent(cursor)}`,
      });

      expect(response.statusCode).toBe(200);

      // Verify cursor was included in query
      const queryCall = mockQuery.mock.calls[0];
      const [sql, params] = queryCall;
      expect(sql).toContain('block_timestamp < $3');
      expect(params).toContain(cursor);
    });

    it('should support sort_by=glm parameter', async () => {
      mockQueryOne.mockResolvedValueOnce({ count: '2' });
      mockQuery.mockResolvedValueOnce(mockTransactions);
      mockQueryOne.mockResolvedValueOnce({ count: '0' });
      mockQueryOne.mockResolvedValueOnce({ count: '0' });

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions?sort_by=glm',
      });

      expect(response.statusCode).toBe(200);

      // Verify sort column in query
      const queryCall = mockQuery.mock.calls[0];
      const [sql] = queryCall;
      expect(sql).toContain('ORDER BY value_glm');
    });

    it('should support sort_by=block parameter', async () => {
      mockQueryOne.mockResolvedValueOnce({ count: '2' });
      mockQuery.mockResolvedValueOnce(mockTransactions);
      mockQueryOne.mockResolvedValueOnce({ count: '0' });
      mockQueryOne.mockResolvedValueOnce({ count: '0' });

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions?sort_by=block',
      });

      expect(response.statusCode).toBe(200);

      // Verify sort column in query
      const queryCall = mockQuery.mock.calls[0];
      const [sql] = queryCall;
      expect(sql).toContain('ORDER BY block_number');
    });

    it('should support sort_order=asc parameter', async () => {
      mockQueryOne.mockResolvedValueOnce({ count: '2' });
      mockQuery.mockResolvedValueOnce(mockTransactions);
      mockQueryOne.mockResolvedValueOnce({ count: '0' });
      mockQueryOne.mockResolvedValueOnce({ count: '0' });

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions?sort_order=asc',
      });

      expect(response.statusCode).toBe(200);

      // Verify sort order in query
      const queryCall = mockQuery.mock.calls[0];
      const [sql] = queryCall;
      expect(sql).toContain('ASC');
    });

    it('should reverse results for prev direction', async () => {
      const reversedTransactions = [...mockTransactions].reverse();

      mockQueryOne.mockResolvedValueOnce({ count: '2' });
      mockQuery.mockResolvedValueOnce(reversedTransactions);
      mockQueryOne.mockResolvedValueOnce({ count: '1' });
      mockQueryOne.mockResolvedValueOnce({ count: '0' });

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions?direction=prev&cursor=2025-01-14T11:00:00.000Z',
      });

      expect(response.statusCode).toBe(200);
      const body = JSON.parse(response.body);

      // Results should be reversed back to original order
      expect(body.transactions[0].tx_hash).toBe('0xabc123');
    });

    it('should set next_cursor when more results exist', async () => {
      mockQueryOne.mockResolvedValueOnce({ count: '10' });
      mockQuery.mockResolvedValueOnce(mockTransactions);
      mockQueryOne.mockResolvedValueOnce({ count: '5' }); // more older records
      mockQueryOne.mockResolvedValueOnce({ count: '0' });

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions',
      });

      const body = JSON.parse(response.body);
      expect(body.next_cursor).not.toBeNull();
    });

    it('should set prev_cursor when navigating forward with results behind', async () => {
      mockQueryOne.mockResolvedValueOnce({ count: '10' });
      mockQuery.mockResolvedValueOnce(mockTransactions);
      mockQueryOne.mockResolvedValueOnce({ count: '0' });
      mockQueryOne.mockResolvedValueOnce({ count: '5' }); // more newer records

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions',
      });

      const body = JSON.parse(response.body);
      expect(body.prev_cursor).not.toBeNull();
    });

    it('should handle empty results', async () => {
      mockQueryOne.mockResolvedValueOnce({ count: '0' });
      mockQuery.mockResolvedValueOnce([]);

      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions',
      });

      expect(response.statusCode).toBe(200);
      const body = JSON.parse(response.body);

      expect(body.transactions).toEqual([]);
      expect(body.total).toBe(0);
      expect(body.next_cursor).toBeNull();
      expect(body.prev_cursor).toBeNull();
    });

    it('should reject limit > 100', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions?limit=101',
      });

      expect(response.statusCode).toBe(400);
    });

    it('should reject limit < 1', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions?limit=0',
      });

      expect(response.statusCode).toBe(400);
    });

    it('should reject invalid sort_by value', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions?sort_by=invalid',
      });

      expect(response.statusCode).toBe(400);
    });

    it('should reject invalid direction value', async () => {
      const response = await app.inject({
        method: 'GET',
        url: '/metrics/transactions?direction=invalid',
      });

      expect(response.statusCode).toBe(400);
    });
  });
});
