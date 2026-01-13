import { FastifyInstance } from "fastify";
import { query, queryOne } from "../db/connection.js";
import { createCacheHooks } from "../cache/redis.js";
import { Transaction, TransactionsResponse } from "../types/index.js";
import { config } from "../config.js";

interface TransactionsQuery {
  limit?: number;
  cursor?: string;
  direction?: "next" | "prev";
  sort_by?: "time" | "glm" | "block";
  sort_order?: "asc" | "desc";
}

interface TransactionRow {
  tx_hash: string;
  block_number: string | number;
  block_timestamp: Date;
  from_address: string;
  to_address: string;
  value_glm: string | number;
  tx_type: string;
}

const transactionsQuerySchema = {
  type: "object",
  properties: {
    limit: { type: "integer", minimum: 1, maximum: 100, default: 10 },
    cursor: { type: "string" },
    direction: { type: "string", enum: ["next", "prev"], default: "next" },
    sort_by: { type: "string", enum: ["time", "glm", "block"], default: "time" },
    sort_order: { type: "string", enum: ["asc", "desc"], default: "desc" },
  },
};

export async function transactionsRoutes(
  fastify: FastifyInstance
): Promise<void> {
  const cacheHooks = createCacheHooks("transactions");

  fastify.get<{ Querystring: TransactionsQuery }>(
    "/metrics/transactions",
    {
      schema: { querystring: transactionsQuerySchema },
      preHandler: cacheHooks.preHandler,
      onSend: cacheHooks.onSend,
    },
    async (request) => {
      const limit = request.query.limit ?? 10;
      const cursor = request.query.cursor;
      const direction = request.query.direction ?? "next";
      const sortBy = request.query.sort_by ?? "time";
      const sortOrder = request.query.sort_order ?? "desc";

      // Only show requester-to-provider transactions (not master-to-requester funding)
      const txTypeFilter = "requester_to_provider";
      const minDate = config.transactionsMinDate;

      // Count total transactions
      const totalRow = await queryOne<{ count: string }>(
        "SELECT COUNT(*) as count FROM glm_transactions WHERE tx_type = $1 AND block_timestamp >= $2",
        [txTypeFilter, minDate]
      );
      const total = parseInt(totalRow?.count ?? "0", 10);

      // Determine sort column
      const sortColumnMap: Record<string, string> = {
        time: "block_timestamp",
        glm: "value_glm",
        block: "block_number",
      };
      const sortColumn = sortColumnMap[sortBy];
      const order = sortOrder.toUpperCase();

      // Helper to get cursor value from a transaction based on sort column
      const getCursorValue = (t: Transaction): string => {
        switch (sortBy) {
          case "glm":
            return String(t.value_glm);
          case "block":
            return String(t.block_number);
          default:
            return t.block_timestamp;
        }
      };

      // Build query
      let sql = `
        SELECT tx_hash, block_number, block_timestamp, from_address, to_address, value_glm, tx_type
        FROM glm_transactions
        WHERE tx_type = $1 AND block_timestamp >= $2
      `;
      const params: unknown[] = [txTypeFilter, minDate];

      if (direction === "next") {
        if (cursor) {
          sql += ` AND ${sortColumn} < $3`;
          params.push(cursor);
        }
      } else {
        if (cursor) {
          sql += ` AND ${sortColumn} > $3`;
          params.push(cursor);
        }
      }

      sql += ` ORDER BY ${sortColumn} ${order} LIMIT $${params.length + 1}`;
      params.push(limit);

      let rows = await query<TransactionRow>(sql, params);

      // Always return newest first for UI consistency
      if (direction === "prev") {
        rows = rows.reverse();
      }

      const pageTransactions: Transaction[] = rows.map((r) => ({
        tx_hash: r.tx_hash,
        block_number:
          typeof r.block_number === "string"
            ? parseInt(r.block_number, 10)
            : r.block_number,
        block_timestamp:
          r.block_timestamp instanceof Date
            ? r.block_timestamp.toISOString()
            : String(r.block_timestamp),
        from_address: r.from_address,
        to_address: r.to_address,
        value_glm: parseFloat(String(r.value_glm)),
        tx_type: r.tx_type,
      }));

      // Determine cursors for navigation
      let nextCursor: string | null = null;
      let prevCursor: string | null = null;

      if (pageTransactions.length > 0) {
        const firstCursor = getCursorValue(pageTransactions[0]);
        const lastCursor = getCursorValue(
          pageTransactions[pageTransactions.length - 1]
        );

        if (direction === "next") {
          // Check if there are older records
          const olderCount = await queryOne<{ count: string }>(
            `SELECT COUNT(*) as count FROM glm_transactions WHERE tx_type = $1 AND block_timestamp >= $2 AND ${sortColumn} < $3`,
            [txTypeFilter, minDate, lastCursor]
          );
          if (parseInt(olderCount?.count ?? "0", 10) > 0) {
            nextCursor = lastCursor;
          }

          // Check if there are newer records
          const newerCount = await queryOne<{ count: string }>(
            `SELECT COUNT(*) as count FROM glm_transactions WHERE tx_type = $1 AND block_timestamp >= $2 AND ${sortColumn} > $3`,
            [txTypeFilter, minDate, firstCursor]
          );
          if (parseInt(newerCount?.count ?? "0", 10) > 0) {
            prevCursor = firstCursor;
          }
        } else {
          if (cursor) {
            // Check if there are newer records
            const newerCount = await queryOne<{ count: string }>(
              `SELECT COUNT(*) as count FROM glm_transactions WHERE tx_type = $1 AND block_timestamp >= $2 AND ${sortColumn} > $3`,
              [txTypeFilter, minDate, firstCursor]
            );
            if (parseInt(newerCount?.count ?? "0", 10) > 0) {
              prevCursor = firstCursor;
            }

            // Check if there are older records
            const olderCount = await queryOne<{ count: string }>(
              `SELECT COUNT(*) as count FROM glm_transactions WHERE tx_type = $1 AND block_timestamp >= $2 AND ${sortColumn} < $3`,
              [txTypeFilter, minDate, lastCursor]
            );
            if (parseInt(olderCount?.count ?? "0", 10) > 0) {
              nextCursor = lastCursor;
            }
          } else {
            // "Last" page - check if there are newer records
            const newerCount = await queryOne<{ count: string }>(
              `SELECT COUNT(*) as count FROM glm_transactions WHERE tx_type = $1 AND block_timestamp >= $2 AND ${sortColumn} > $3`,
              [txTypeFilter, minDate, firstCursor]
            );
            if (parseInt(newerCount?.count ?? "0", 10) > 0) {
              prevCursor = firstCursor;
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
