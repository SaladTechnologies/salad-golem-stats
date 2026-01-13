import { query } from '../db/connection.js';
import type {
  PlanPeriod,
  Granularity,
  PlanTotals,
  PlanDataPoint,
  GroupedMetric,
  PlanStatsResponse,
} from '../types/index.js';

// Data offset in hours - can't return data that hasn't gone through Golem yet
const DATA_OFFSET_HOURS = 48; // 2 days

// Period to hours mapping
const PERIOD_HOURS: Record<PlanPeriod, number | null> = {
  '6h': 6,
  '24h': 24,
  '7d': 168,
  '30d': 720,
  '90d': 2160,
  'total': null, // No limit
};

// Determine granularity based on period
function getGranularity(period: PlanPeriod): Granularity {
  // 7 days or less = hourly, otherwise daily
  const hours = PERIOD_HOURS[period];
  if (hours === null || hours > 168) {
    return 'daily';
  }
  return 'hourly';
}

// Get the data cutoff timestamp (now minus offset)
function getDataCutoff(): Date {
  const cutoff = new Date();
  cutoff.setHours(cutoff.getHours() - DATA_OFFSET_HOURS);
  return cutoff;
}

// Get the range start timestamp based on period
function getRangeStart(cutoff: Date, period: PlanPeriod): Date | null {
  const hours = PERIOD_HOURS[period];
  if (hours === null) {
    return null; // 'total' means no start limit
  }
  const start = new Date(cutoff);
  start.setHours(start.getHours() - hours);
  return start;
}

// SQL timestamp format helper (for epoch milliseconds)
function toEpochMs(date: Date): number {
  return date.getTime();
}

interface TotalsRow {
  total_fees: string | null;
  compute_hours: string | null;
  core_hours: string | null;
  ram_hours: string | null;
  gpu_hours: string | null;
}

interface TimeSeriesRow {
  bucket: Date;
  active_nodes: string;
  total_fees: string | null;
  compute_hours: string | null;
  core_hours: string | null;
  ram_hours: string | null;
  gpu_hours: string | null;
}

interface GpuGroupRow {
  group_name: string;
  value: string | null;
}

interface GpuTimeSeriesRow {
  bucket: Date;
  group_name: string;
  value: string | null;
}

interface GroupedTimeSeriesOutput {
  labels: string[];
  datasets: { label: string; data: number[] }[];
}

// Transform raw time-series rows into chart-ready format
function transformToGroupedTimeSeries(
  rows: GpuTimeSeriesRow[],
  timeSeries: PlanDataPoint[]
): GroupedTimeSeriesOutput {
  // Get ordered timestamps from the main time series
  const labels = timeSeries.map((p) => p.timestamp);
  const timestampIndex = new Map(labels.map((ts, i) => [ts, i]));

  // Group values by group_name
  const groupData = new Map<string, number[]>();
  const groupTotals = new Map<string, number>();

  for (const row of rows) {
    const ts = new Date(row.bucket.getTime() + (DATA_OFFSET_HOURS * 60 * 60 * 1000)).toISOString();
    const idx = timestampIndex.get(ts);
    if (idx === undefined) continue;

    const group = row.group_name;
    const value = parseFloat(row.value || '0');

    if (!groupData.has(group)) {
      groupData.set(group, new Array(labels.length).fill(0));
      groupTotals.set(group, 0);
    }
    groupData.get(group)![idx] = value;
    groupTotals.set(group, (groupTotals.get(group) || 0) + value);
  }

  // Sort groups by total value descending, take top 6
  const sortedGroups = [...groupData.entries()]
    .sort((a, b) => (groupTotals.get(b[0]) || 0) - (groupTotals.get(a[0]) || 0))
    .slice(0, 6);

  const datasets = sortedGroups.map(([label, data]) => ({ label, data }));

  return { labels, datasets };
}

export async function getPlanStats(period: PlanPeriod): Promise<PlanStatsResponse> {
  const cutoff = getDataCutoff();
  const rangeStart = getRangeStart(cutoff, period);
  const granularity = getGranularity(period);

  const cutoffMs = toEpochMs(cutoff);
  const startMs = rangeStart ? toEpochMs(rangeStart) : null;

  // Build WHERE clause for time range - include running jobs
  const timeWhere = startMs
    ? 'start_at < $1 AND (stop_at IS NULL OR stop_at >= $2)'
    : 'start_at < $1';
  const timeParams = startMs ? [cutoffMs, startMs] : [cutoffMs];

  // 1. Get totals
  // Active nodes uses overlap logic: nodes running at any point during the range
  // All metrics sum across jobs in the time range
  const totalsQuery = `
    SELECT
      COALESCE(SUM(invoice_amount), 0) as total_fees,
      COALESCE(SUM((COALESCE(stop_at, $1) - start_at) / 1000.0 / 3600.0), 0) as compute_hours,
      COALESCE(SUM(cpu * (COALESCE(stop_at, $1) - start_at) / 1000.0 / 3600.0), 0) as core_hours,
      COALESCE(SUM(ram * (COALESCE(stop_at, $1) - start_at) / 1000.0 / 3600.0 / 1024.0), 0) as ram_hours,
      COALESCE(SUM(
        CASE WHEN gpu_class_id IS NOT NULL AND gpu_class_id != ''
        THEN (COALESCE(stop_at, $1) - start_at) / 1000.0 / 3600.0
        ELSE 0 END
      ), 0) as gpu_hours
    FROM node_plan
    WHERE ${timeWhere}
  `;

  // Active nodes: count nodes that were running at any point during the range
  const activeNodesQuery = startMs
    ? `
    SELECT COUNT(DISTINCT node_id) as active_nodes
    FROM node_plan
    WHERE start_at < $1 AND (stop_at IS NULL OR stop_at >= $2)
  `
    : `
    SELECT COUNT(DISTINCT node_id) as active_nodes
    FROM node_plan
    WHERE start_at < $1
  `;

  // 2. Get time series
  // All metrics use overlap logic - hours are distributed across buckets where jobs were running
  // overlap_hours = LEAST(stop_at, bucket_end) - GREATEST(start_at, bucket_start)
  const bucketInterval = granularity === 'hourly' ? 'hour' : 'day';
  const intervalStr = granularity === 'hourly' ? '1 hour' : '1 day';
  const msPerHour = 3600000;

  const timeSeriesQuery = startMs
    ? `
    WITH buckets AS (
      SELECT
        bucket,
        (EXTRACT(EPOCH FROM bucket) * 1000)::bigint as bucket_start_ms,
        (EXTRACT(EPOCH FROM bucket + interval '${intervalStr}') * 1000)::bigint as bucket_end_ms
      FROM generate_series(
        date_trunc('${bucketInterval}', to_timestamp($2 / 1000.0)),
        date_trunc('${bucketInterval}', to_timestamp($1 / 1000.0)),
        interval '${intervalStr}'
      ) as bucket
    ),
    -- Calculate overlap duration for each job in each bucket
    job_overlaps AS (
      SELECT
        b.bucket,
        b.bucket_start_ms,
        b.bucket_end_ms,
        np.node_id,
        np.cpu,
        np.ram,
        np.gpu_class_id,
        np.invoice_amount,
        np.start_at,
        np.stop_at,
        -- Job duration in ms (use cutoff for running jobs)
        GREATEST(1, COALESCE(np.stop_at, $1) - np.start_at) as job_duration_ms,
        -- Overlap duration in ms: min(stop, bucket_end) - max(start, bucket_start)
        GREATEST(0, LEAST(COALESCE(np.stop_at, $1), b.bucket_end_ms) - GREATEST(np.start_at, b.bucket_start_ms)) as overlap_ms
      FROM buckets b
      JOIN node_plan np ON
        np.start_at < b.bucket_end_ms
        AND (np.stop_at IS NULL OR np.stop_at >= b.bucket_start_ms)
        AND np.start_at < $1
    )
    SELECT
      bucket,
      COUNT(DISTINCT node_id) as active_nodes,
      -- Fees: distributed proportionally across job duration
      COALESCE(SUM(invoice_amount * overlap_ms / job_duration_ms), 0) as total_fees,
      COALESCE(SUM(overlap_ms / ${msPerHour}.0), 0) as compute_hours,
      COALESCE(SUM(cpu * overlap_ms / ${msPerHour}.0), 0) as core_hours,
      COALESCE(SUM(ram * overlap_ms / ${msPerHour}.0 / 1024.0), 0) as ram_hours,
      COALESCE(SUM(
        CASE WHEN gpu_class_id IS NOT NULL AND gpu_class_id != ''
        THEN overlap_ms / ${msPerHour}.0
        ELSE 0 END
      ), 0) as gpu_hours
    FROM job_overlaps
    GROUP BY bucket, bucket_start_ms, bucket_end_ms
    ORDER BY bucket
  `
    : `
    WITH all_buckets AS (
      SELECT
        bucket,
        (EXTRACT(EPOCH FROM bucket) * 1000)::bigint as bucket_start_ms,
        (EXTRACT(EPOCH FROM bucket + interval '${intervalStr}') * 1000)::bigint as bucket_end_ms
      FROM (
        SELECT DISTINCT date_trunc('${bucketInterval}', to_timestamp(COALESCE(stop_at, $1) / 1000.0)) as bucket
        FROM node_plan WHERE start_at < $1
      ) x
    ),
    job_overlaps AS (
      SELECT
        b.bucket,
        b.bucket_start_ms,
        b.bucket_end_ms,
        np.node_id,
        np.cpu,
        np.ram,
        np.gpu_class_id,
        np.invoice_amount,
        np.start_at,
        np.stop_at,
        GREATEST(1, COALESCE(np.stop_at, $1) - np.start_at) as job_duration_ms,
        GREATEST(0, LEAST(COALESCE(np.stop_at, $1), b.bucket_end_ms) - GREATEST(np.start_at, b.bucket_start_ms)) as overlap_ms
      FROM all_buckets b
      JOIN node_plan np ON
        np.start_at < b.bucket_end_ms
        AND (np.stop_at IS NULL OR np.stop_at >= b.bucket_start_ms)
        AND np.start_at < $1
    )
    SELECT
      bucket,
      COUNT(DISTINCT node_id) as active_nodes,
      COALESCE(SUM(invoice_amount * overlap_ms / job_duration_ms), 0) as total_fees,
      COALESCE(SUM(overlap_ms / ${msPerHour}.0), 0) as compute_hours,
      COALESCE(SUM(cpu * overlap_ms / ${msPerHour}.0), 0) as core_hours,
      COALESCE(SUM(ram * overlap_ms / ${msPerHour}.0 / 1024.0), 0) as ram_hours,
      COALESCE(SUM(
        CASE WHEN gpu_class_id IS NOT NULL AND gpu_class_id != ''
        THEN overlap_ms / ${msPerHour}.0
        ELSE 0 END
      ), 0) as gpu_hours
    FROM job_overlaps
    GROUP BY bucket, bucket_start_ms, bucket_end_ms
    ORDER BY bucket
  `;

  // 3. Get GPU hours by model
  const gpuHoursByModelQuery = `
    SELECT
      COALESCE(gc.gpu_class_name, 'Unknown') as group_name,
      COALESCE(SUM((COALESCE(np.stop_at, $1) - np.start_at) / 1000.0 / 3600.0), 0) as value
    FROM node_plan np
    LEFT JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    WHERE ${timeWhere}
      AND np.gpu_class_id IS NOT NULL AND np.gpu_class_id != ''
    GROUP BY gc.gpu_class_name
    ORDER BY value DESC
  `;

  // 4. Get GPU hours by VRAM
  const gpuHoursByVramQuery = `
    SELECT
      COALESCE(gc.vram_gb::text || ' GB', 'Unknown') as group_name,
      COALESCE(SUM((COALESCE(np.stop_at, $1) - np.start_at) / 1000.0 / 3600.0), 0) as value
    FROM node_plan np
    LEFT JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    WHERE ${timeWhere}
      AND np.gpu_class_id IS NOT NULL AND np.gpu_class_id != ''
    GROUP BY gc.vram_gb
    ORDER BY gc.vram_gb
  `;

  // 5. Get active nodes by GPU model (using overlap logic)
  const activeNodesByModelQuery = startMs
    ? `
    SELECT
      COALESCE(gc.gpu_class_name, 'No GPU') as group_name,
      COUNT(DISTINCT np.node_id)::text as value
    FROM node_plan np
    LEFT JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    WHERE np.start_at < $1 AND (np.stop_at IS NULL OR np.stop_at >= $2)
    GROUP BY gc.gpu_class_name
    ORDER BY value DESC
  `
    : `
    SELECT
      COALESCE(gc.gpu_class_name, 'No GPU') as group_name,
      COUNT(DISTINCT np.node_id)::text as value
    FROM node_plan np
    LEFT JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    WHERE np.start_at < $1
    GROUP BY gc.gpu_class_name
    ORDER BY value DESC
  `;

  // 6. Get active nodes by VRAM (using overlap logic)
  const activeNodesByVramQuery = startMs
    ? `
    SELECT
      COALESCE(gc.vram_gb::text || ' GB', 'No GPU') as group_name,
      COUNT(DISTINCT np.node_id)::text as value
    FROM node_plan np
    LEFT JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    WHERE np.start_at < $1 AND (np.stop_at IS NULL OR np.stop_at >= $2)
    GROUP BY gc.vram_gb
    ORDER BY gc.vram_gb NULLS FIRST
  `
    : `
    SELECT
      COALESCE(gc.vram_gb::text || ' GB', 'No GPU') as group_name,
      COUNT(DISTINCT np.node_id)::text as value
    FROM node_plan np
    LEFT JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    WHERE np.start_at < $1
    GROUP BY gc.vram_gb
    ORDER BY gc.vram_gb NULLS FIRST
  `;

  // 7. Get GPU hours by model TIME SERIES (for stacked charts) - uses overlap logic
  const gpuHoursByModelTsQuery = startMs
    ? `
    WITH buckets AS (
      SELECT
        bucket,
        (EXTRACT(EPOCH FROM bucket) * 1000)::bigint as bucket_start_ms,
        (EXTRACT(EPOCH FROM bucket + interval '${intervalStr}') * 1000)::bigint as bucket_end_ms
      FROM generate_series(
        date_trunc('${bucketInterval}', to_timestamp($2 / 1000.0)),
        date_trunc('${bucketInterval}', to_timestamp($1 / 1000.0)),
        interval '${intervalStr}'
      ) as bucket
    )
    SELECT
      b.bucket,
      gc.gpu_class_name as group_name,
      COALESCE(SUM(GREATEST(0, LEAST(COALESCE(np.stop_at, $1), b.bucket_end_ms) - GREATEST(np.start_at, b.bucket_start_ms)) / ${msPerHour}.0), 0) as value
    FROM buckets b
    JOIN node_plan np ON
      np.start_at < b.bucket_end_ms
      AND (np.stop_at IS NULL OR np.stop_at >= b.bucket_start_ms)
      AND np.start_at < $1
      AND np.gpu_class_id IS NOT NULL AND np.gpu_class_id != ''
    JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    GROUP BY b.bucket, gc.gpu_class_name
    ORDER BY b.bucket, value DESC
  `
    : `
    WITH all_buckets AS (
      SELECT
        bucket,
        (EXTRACT(EPOCH FROM bucket) * 1000)::bigint as bucket_start_ms,
        (EXTRACT(EPOCH FROM bucket + interval '${intervalStr}') * 1000)::bigint as bucket_end_ms
      FROM (SELECT DISTINCT date_trunc('${bucketInterval}', to_timestamp(COALESCE(stop_at, $1) / 1000.0)) as bucket FROM node_plan WHERE start_at < $1) x
    )
    SELECT
      b.bucket,
      gc.gpu_class_name as group_name,
      COALESCE(SUM(GREATEST(0, LEAST(COALESCE(np.stop_at, $1), b.bucket_end_ms) - GREATEST(np.start_at, b.bucket_start_ms)) / ${msPerHour}.0), 0) as value
    FROM all_buckets b
    JOIN node_plan np ON
      np.start_at < b.bucket_end_ms
      AND (np.stop_at IS NULL OR np.stop_at >= b.bucket_start_ms)
      AND np.start_at < $1
      AND np.gpu_class_id IS NOT NULL AND np.gpu_class_id != ''
    JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    GROUP BY b.bucket, gc.gpu_class_name
    ORDER BY b.bucket, value DESC
  `;

  // 8. Get GPU hours by VRAM TIME SERIES - uses overlap logic
  const gpuHoursByVramTsQuery = startMs
    ? `
    WITH buckets AS (
      SELECT
        bucket,
        (EXTRACT(EPOCH FROM bucket) * 1000)::bigint as bucket_start_ms,
        (EXTRACT(EPOCH FROM bucket + interval '${intervalStr}') * 1000)::bigint as bucket_end_ms
      FROM generate_series(
        date_trunc('${bucketInterval}', to_timestamp($2 / 1000.0)),
        date_trunc('${bucketInterval}', to_timestamp($1 / 1000.0)),
        interval '${intervalStr}'
      ) as bucket
    )
    SELECT
      b.bucket,
      gc.vram_gb::text || ' GB' as group_name,
      COALESCE(SUM(GREATEST(0, LEAST(COALESCE(np.stop_at, $1), b.bucket_end_ms) - GREATEST(np.start_at, b.bucket_start_ms)) / ${msPerHour}.0), 0) as value
    FROM buckets b
    JOIN node_plan np ON
      np.start_at < b.bucket_end_ms
      AND (np.stop_at IS NULL OR np.stop_at >= b.bucket_start_ms)
      AND np.start_at < $1
      AND np.gpu_class_id IS NOT NULL AND np.gpu_class_id != ''
    JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    WHERE gc.vram_gb IS NOT NULL
    GROUP BY b.bucket, gc.vram_gb
    ORDER BY b.bucket, gc.vram_gb
  `
    : `
    WITH all_buckets AS (
      SELECT
        bucket,
        (EXTRACT(EPOCH FROM bucket) * 1000)::bigint as bucket_start_ms,
        (EXTRACT(EPOCH FROM bucket + interval '${intervalStr}') * 1000)::bigint as bucket_end_ms
      FROM (SELECT DISTINCT date_trunc('${bucketInterval}', to_timestamp(COALESCE(stop_at, $1) / 1000.0)) as bucket FROM node_plan WHERE start_at < $1) x
    )
    SELECT
      b.bucket,
      gc.vram_gb::text || ' GB' as group_name,
      COALESCE(SUM(GREATEST(0, LEAST(COALESCE(np.stop_at, $1), b.bucket_end_ms) - GREATEST(np.start_at, b.bucket_start_ms)) / ${msPerHour}.0), 0) as value
    FROM all_buckets b
    JOIN node_plan np ON
      np.start_at < b.bucket_end_ms
      AND (np.stop_at IS NULL OR np.stop_at >= b.bucket_start_ms)
      AND np.start_at < $1
      AND np.gpu_class_id IS NOT NULL AND np.gpu_class_id != ''
    JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    WHERE gc.vram_gb IS NOT NULL
    GROUP BY b.bucket, gc.vram_gb
    ORDER BY b.bucket, gc.vram_gb
  `;

  // 9. Get active nodes by GPU model TIME SERIES - excludes non-GPU workloads
  // Uses overlap logic: count nodes running during each bucket
  const activeNodesByModelTsQuery = startMs
    ? `
    WITH buckets AS (
      SELECT generate_series(
        date_trunc('${bucketInterval}', to_timestamp($2 / 1000.0)),
        date_trunc('${bucketInterval}', to_timestamp($1 / 1000.0)),
        interval '${intervalStr}'
      ) as bucket
    )
    SELECT
      b.bucket,
      gc.gpu_class_name as group_name,
      COUNT(DISTINCT np.node_id)::text as value
    FROM buckets b
    CROSS JOIN gpu_classes gc
    LEFT JOIN node_plan np ON
      np.gpu_class_id = gc.gpu_class_id
      AND np.start_at < (EXTRACT(EPOCH FROM (b.bucket + interval '${intervalStr}')) * 1000)
      AND (np.stop_at IS NULL OR np.stop_at >= (EXTRACT(EPOCH FROM b.bucket) * 1000))
      AND np.start_at < $1
    WHERE gc.gpu_class_id IS NOT NULL
    GROUP BY b.bucket, gc.gpu_class_name
    HAVING COUNT(DISTINCT np.node_id) > 0
    ORDER BY b.bucket, value DESC
  `
    : `
    WITH all_buckets AS (
      SELECT DISTINCT date_trunc('${bucketInterval}', to_timestamp(COALESCE(stop_at, $1) / 1000.0)) as bucket
      FROM node_plan WHERE start_at < $1
    )
    SELECT
      b.bucket,
      gc.gpu_class_name as group_name,
      COUNT(DISTINCT np.node_id)::text as value
    FROM all_buckets b
    CROSS JOIN gpu_classes gc
    LEFT JOIN node_plan np ON
      np.gpu_class_id = gc.gpu_class_id
      AND np.start_at < (EXTRACT(EPOCH FROM (b.bucket + interval '${intervalStr}')) * 1000)
      AND (np.stop_at IS NULL OR np.stop_at >= (EXTRACT(EPOCH FROM b.bucket) * 1000))
      AND np.start_at < $1
    WHERE gc.gpu_class_id IS NOT NULL
    GROUP BY b.bucket, gc.gpu_class_name
    HAVING COUNT(DISTINCT np.node_id) > 0
    ORDER BY b.bucket, value DESC
  `;

  // 10. Get active nodes by VRAM TIME SERIES - excludes non-GPU workloads
  // Uses overlap logic: count nodes running during each bucket
  const activeNodesByVramTsQuery = startMs
    ? `
    WITH buckets AS (
      SELECT generate_series(
        date_trunc('${bucketInterval}', to_timestamp($2 / 1000.0)),
        date_trunc('${bucketInterval}', to_timestamp($1 / 1000.0)),
        interval '${intervalStr}'
      ) as bucket
    ),
    vram_groups AS (
      SELECT DISTINCT vram_gb FROM gpu_classes WHERE vram_gb IS NOT NULL
    )
    SELECT
      b.bucket,
      vg.vram_gb::text || ' GB' as group_name,
      COUNT(DISTINCT np.node_id)::text as value
    FROM buckets b
    CROSS JOIN vram_groups vg
    LEFT JOIN gpu_classes gc ON gc.vram_gb = vg.vram_gb
    LEFT JOIN node_plan np ON
      np.gpu_class_id = gc.gpu_class_id
      AND np.start_at < (EXTRACT(EPOCH FROM (b.bucket + interval '${intervalStr}')) * 1000)
      AND (np.stop_at IS NULL OR np.stop_at >= (EXTRACT(EPOCH FROM b.bucket) * 1000))
      AND np.start_at < $1
    GROUP BY b.bucket, vg.vram_gb
    HAVING COUNT(DISTINCT np.node_id) > 0
    ORDER BY b.bucket, vg.vram_gb
  `
    : `
    WITH all_buckets AS (
      SELECT DISTINCT date_trunc('${bucketInterval}', to_timestamp(COALESCE(stop_at, $1) / 1000.0)) as bucket
      FROM node_plan WHERE start_at < $1
    ),
    vram_groups AS (
      SELECT DISTINCT vram_gb FROM gpu_classes WHERE vram_gb IS NOT NULL
    )
    SELECT
      b.bucket,
      vg.vram_gb::text || ' GB' as group_name,
      COUNT(DISTINCT np.node_id)::text as value
    FROM all_buckets b
    CROSS JOIN vram_groups vg
    LEFT JOIN gpu_classes gc ON gc.vram_gb = vg.vram_gb
    LEFT JOIN node_plan np ON
      np.gpu_class_id = gc.gpu_class_id
      AND np.start_at < (EXTRACT(EPOCH FROM (b.bucket + interval '${intervalStr}')) * 1000)
      AND (np.stop_at IS NULL OR np.stop_at >= (EXTRACT(EPOCH FROM b.bucket) * 1000))
      AND np.start_at < $1
    GROUP BY b.bucket, vg.vram_gb
    HAVING COUNT(DISTINCT np.node_id) > 0
    ORDER BY b.bucket, vg.vram_gb
  `;

  // Execute ALL queries in parallel for maximum efficiency
  const [
    totalsResult,
    activeNodesResult,
    timeSeriesResult,
    gpuHoursByModelResult,
    gpuHoursByVramResult,
    activeNodesByModelResult,
    activeNodesByVramResult,
    gpuHoursByModelTsResult,
    gpuHoursByVramTsResult,
    activeNodesByModelTsResult,
    activeNodesByVramTsResult,
  ] = await Promise.all([
    query<TotalsRow>(totalsQuery, timeParams),
    query<{ active_nodes: string }>(activeNodesQuery, timeParams),
    query<TimeSeriesRow>(timeSeriesQuery, timeParams),
    query<GpuGroupRow>(gpuHoursByModelQuery, timeParams),
    query<GpuGroupRow>(gpuHoursByVramQuery, timeParams),
    query<GpuGroupRow>(activeNodesByModelQuery, timeParams),
    query<GpuGroupRow>(activeNodesByVramQuery, timeParams),
    query<GpuTimeSeriesRow>(gpuHoursByModelTsQuery, timeParams),
    query<GpuTimeSeriesRow>(gpuHoursByVramTsQuery, timeParams),
    query<GpuTimeSeriesRow>(activeNodesByModelTsQuery, timeParams),
    query<GpuTimeSeriesRow>(activeNodesByVramTsQuery, timeParams),
  ]);

  // Process totals
  const totalsRow = totalsResult[0];
  const activeNodesRow = activeNodesResult[0];
  const totals: PlanTotals = {
    active_nodes: parseInt(activeNodesRow.active_nodes, 10) || 0,
    total_fees: parseFloat(totalsRow.total_fees || '0'),
    compute_hours: parseFloat(totalsRow.compute_hours || '0'),
    core_hours: parseFloat(totalsRow.core_hours || '0'),
    ram_hours: parseFloat(totalsRow.ram_hours || '0'),
    gpu_hours: parseFloat(totalsRow.gpu_hours || '0'),
  };

  // Process time series
  const timeSeries: PlanDataPoint[] = timeSeriesResult.map((row) => ({
    timestamp: new Date(row.bucket.getTime() + (DATA_OFFSET_HOURS * 60 * 60 * 1000)).toISOString(),
    active_nodes: parseInt(row.active_nodes, 10) || 0,
    total_fees: parseFloat(row.total_fees || '0'),
    compute_hours: parseFloat(row.compute_hours || '0'),
    core_hours: parseFloat(row.core_hours || '0'),
    ram_hours: parseFloat(row.ram_hours || '0'),
    gpu_hours: parseFloat(row.gpu_hours || '0'),
  }));

  // Process grouped metrics
  const gpuHoursByModel: GroupedMetric[] = gpuHoursByModelResult.map((row) => ({
    group: row.group_name,
    value: parseFloat(row.value || '0'),
  }));

  const gpuHoursByVram: GroupedMetric[] = gpuHoursByVramResult.map((row) => ({
    group: row.group_name,
    value: parseFloat(row.value || '0'),
  }));

  const activeNodesByGpuModel: GroupedMetric[] = activeNodesByModelResult.map((row) => ({
    group: row.group_name,
    value: parseFloat(row.value || '0'),
  }));

  const activeNodesByVram: GroupedMetric[] = activeNodesByVramResult.map((row) => ({
    group: row.group_name,
    value: parseFloat(row.value || '0'),
  }));

  // Process grouped time series (depends on timeSeries for bucket mapping)
  const gpuHoursByModelTs = transformToGroupedTimeSeries(gpuHoursByModelTsResult, timeSeries);
  const gpuHoursByVramTs = transformToGroupedTimeSeries(gpuHoursByVramTsResult, timeSeries);
  const activeNodesByModelTs = transformToGroupedTimeSeries(activeNodesByModelTsResult, timeSeries);
  const activeNodesByVramTs = transformToGroupedTimeSeries(activeNodesByVramTsResult, timeSeries);

  return {
    period,
    granularity,
    data_cutoff: cutoff.toISOString(),
    range: {
      start: rangeStart ? rangeStart.toISOString() : 'beginning',
      end: cutoff.toISOString(),
    },
    totals,
    gpu_hours_by_model: gpuHoursByModel,
    gpu_hours_by_vram: gpuHoursByVram,
    active_nodes_by_gpu_model: activeNodesByGpuModel,
    active_nodes_by_vram: activeNodesByVram,
    time_series: timeSeries,
    gpu_hours_by_model_ts: gpuHoursByModelTs,
    gpu_hours_by_vram_ts: gpuHoursByVramTs,
    active_nodes_by_gpu_model_ts: activeNodesByModelTs,
    active_nodes_by_vram_ts: activeNodesByVramTs,
  };
}
