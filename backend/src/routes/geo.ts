import { FastifyInstance } from 'fastify';
import { latLngToCell, cellToLatLng } from 'h3-js';
import { query } from '../db/connection.js';
import { createCacheHooks } from '../cache/redis.js';
import { GeoPoint, CityCount } from '../types/index.js';

interface CityRow {
  name: string;
  count: number;
  lat: number;
  long: number;
}

interface GeoCountsQuery {
  resolution?: number;
}

const geoCountsQuerySchema = {
  type: 'object',
  properties: {
    resolution: { type: 'integer', minimum: 0, maximum: 15, default: 4 },
  },
};

export async function geoRoutes(fastify: FastifyInstance): Promise<void> {
  const geoCacheHooks = createCacheHooks('geo_counts');
  const cityCacheHooks = createCacheHooks('city_counts');

  fastify.get<{ Querystring: GeoCountsQuery }>(
    '/metrics/geo_counts',
    {
      schema: { querystring: geoCountsQuerySchema },
      preHandler: geoCacheHooks.preHandler,
      onSend: geoCacheHooks.onSend,
    },
    async (request) => {
      const resolution = request.query.resolution ?? 4;

      const rows = await query<CityRow>(`
        SELECT name, count, lat, long
        FROM city_snapshots
        WHERE ts = (SELECT MAX(ts) FROM city_snapshots)
        ORDER BY count DESC
      `);

      // Backend aggregation: group cities by H3 hexagon
      const hexCounts = new Map<string, number>();
      let maxCount = 0;

      for (const r of rows) {
        const hexId = latLngToCell(r.lat, r.long, resolution);
        const newCount = (hexCounts.get(hexId) ?? 0) + r.count;
        hexCounts.set(hexId, newCount);
        maxCount = Math.max(maxCount, newCount);
      }

      // Sort by count descending
      const sortedHexes = Array.from(hexCounts.entries()).sort((a, b) => b[1] - a[1]);

      // Convert H3 hex IDs to center points
      const result: GeoPoint[] = [];

      for (const [hexId, count] of sortedHexes) {
        try {
          const [centerLat, centerLng] = cellToLatLng(hexId);
          const normalizedCount = maxCount > 0 ? count / maxCount : 0;

          result.push({
            lat: centerLat,
            lng: centerLng,
            normalized: normalizedCount,
          });
        } catch (err) {
          console.error(`[ERROR] Failed to process hex ${hexId}:`, err);
        }
      }

      return result;
    }
  );

  fastify.get(
    '/metrics/city_counts',
    {
      preHandler: cityCacheHooks.preHandler,
      onSend: cityCacheHooks.onSend,
    },
    async () => {
      const rows = await query<CityRow>(`
        SELECT name, count, lat, long
        FROM city_snapshots
        WHERE ts = (SELECT MAX(ts) FROM city_snapshots)
      `);

      const result: CityCount[] = rows.map((r) => ({
        city: r.name,
        count: r.count,
        lat: r.lat,
        lon: r.long,
      }));

      return result;
    }
  );
}
