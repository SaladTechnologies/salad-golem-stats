import { FastifyInstance } from 'fastify';
import { query, queryOne } from '../db/connection.js';
import { createCacheHooks } from '../cache/redis.js';
import { Transaction, TransactionsResponse } from '../types/index.js';

interface TransactionsQuery {
  limit?: number;
  cursor?: string;
  direction?: 'next' | 'prev';
  sort_by?: 'time' | 'glm' | 'usd';
  sort_order?: 'asc' | 'desc';
}

interface TransactionRow {
  ts: Date;
  provider_wallet: string;
  requester_wallet: string;
  tx: string;
  gpu: string;
  ram: number;
  vcpus: number;
  duration: string;
  invoiced_glm: string | number;
  invoiced_dollar: string | number;
}

const transactionsQuerySchema = {
  type: 'object',
  properties: {
    limit: { type: 'integer', minimum: 1, maximum: 100, default: 10 },
    cursor: { type: 'string' },
    direction: { type: 'string', enum: ['next', 'prev'], default: 'next' },
    sort_by: { type: 'string', enum: ['time', 'glm', 'usd'], default: 'time' },
    sort_order: { type: 'string', enum: ['asc', 'desc'], default: 'desc' },
  },
};

export async function transactionsRoutes(fastify: FastifyInstance): Promise<void> {
  const cacheHooks = createCacheHooks('transactions');

  fastify.get<{ Querystring: TransactionsQuery }>(
    '/metrics/transactions',
    {
      schema: { querystring: transactionsQuerySchema },
      preHandler: cacheHooks.preHandler,
      onSend: cacheHooks.onSend,
    },
    async (request) => {
      const limit = request.query.limit ?? 10;
      const cursor = request.query.cursor;
      const direction = request.query.direction ?? 'next';
      const sortBy = request.query.sort_by ?? 'time';
      const sortOrder = request.query.sort_order ?? 'desc';

      // Count total transactions
      const totalRow = await queryOne<{ count: string }>('SELECT COUNT(*) as count FROM placeholder_transactions');
      const total = parseInt(totalRow?.count ?? '0', 10);

      // Determine sort column
      const sortColumnMap: Record<string, string> = {
        time: 'ts',
        glm: 'invoiced_glm',
        usd: 'invoiced_dollar',
      };
      const sortColumn = sortColumnMap[sortBy];
      const order = sortOrder.toUpperCase();

      // Build query
      let sql = `
        SELECT ts, provider_wallet, requester_wallet, tx, gpu, ram, vcpus, duration, invoiced_glm, invoiced_dollar
        FROM placeholder_transactions
      `;
      const params: unknown[] = [];

      if (direction === 'next') {
        if (cursor) {
          sql += ` WHERE ${sortColumn} < $1`;
          params.push(cursor);
        }
      } else {
        if (cursor) {
          sql += ` WHERE ${sortColumn} > $1`;
          params.push(cursor);
        }
      }

      sql += ` ORDER BY ${sortColumn} ${order} LIMIT $${params.length + 1}`;
      params.push(limit);

      let rows = await query<TransactionRow>(sql, params);

      // Always return newest first for UI consistency
      if (direction === 'prev') {
        rows = rows.reverse();
      }

      const pageTransactions: Transaction[] = rows.map((r) => ({
        ts: r.ts instanceof Date ? r.ts.toISOString() : String(r.ts),
        provider_wallet: r.provider_wallet,
        requester_wallet: r.requester_wallet,
        tx: r.tx,
        gpu: r.gpu,
        ram: r.ram,
        vcpus: r.vcpus,
        duration: String(r.duration),
        invoiced_glm: parseFloat(String(r.invoiced_glm)),
        invoiced_dollar: parseFloat(String(r.invoiced_dollar)),
      }));

      // Determine cursors for navigation
      let nextCursor: string | null = null;
      let prevCursor: string | null = null;

      if (pageTransactions.length > 0) {
        if (direction === 'next') {
          // Check if there are older records
          const olderCount = await queryOne<{ count: string }>(
            'SELECT COUNT(*) as count FROM placeholder_transactions WHERE ts < $1',
            [pageTransactions[pageTransactions.length - 1].ts]
          );
          if (parseInt(olderCount?.count ?? '0', 10) > 0) {
            nextCursor = pageTransactions[pageTransactions.length - 1].ts;
          }

          // Check if there are newer records
          const newerCount = await queryOne<{ count: string }>(
            'SELECT COUNT(*) as count FROM placeholder_transactions WHERE ts > $1',
            [pageTransactions[0].ts]
          );
          if (parseInt(newerCount?.count ?? '0', 10) > 0) {
            prevCursor = pageTransactions[0].ts;
          }
        } else {
          if (cursor) {
            // Check if there are newer records
            const newerCount = await queryOne<{ count: string }>(
              'SELECT COUNT(*) as count FROM placeholder_transactions WHERE ts > $1',
              [pageTransactions[0].ts]
            );
            if (parseInt(newerCount?.count ?? '0', 10) > 0) {
              prevCursor = pageTransactions[0].ts;
            }

            // Check if there are older records
            const olderCount = await queryOne<{ count: string }>(
              'SELECT COUNT(*) as count FROM placeholder_transactions WHERE ts < $1',
              [pageTransactions[pageTransactions.length - 1].ts]
            );
            if (parseInt(olderCount?.count ?? '0', 10) > 0) {
              nextCursor = pageTransactions[pageTransactions.length - 1].ts;
            }
          } else {
            // "Last" page - check if there are newer records
            const newerCount = await queryOne<{ count: string }>(
              'SELECT COUNT(*) as count FROM placeholder_transactions WHERE ts > $1',
              [pageTransactions[0].ts]
            );
            if (parseInt(newerCount?.count ?? '0', 10) > 0) {
              prevCursor = pageTransactions[0].ts;
            }
          }
        }
      }

      const response: TransactionsResponse = {
        transactions: pageTransactions,
        next_cursor: nextCursor,
        prev_cursor: prevCursor,
        total,
      };

      return response;
    }
  );
}
