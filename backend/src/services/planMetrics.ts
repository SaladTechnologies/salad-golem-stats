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
  active_nodes: string;
  total_fees: string | null;
  compute_hours: string | null;
  transactions: string;
  core_hours: string | null;
  ram_hours: string | null;
  gpu_hours: string | null;
}

interface TimeSeriesRow {
  bucket: Date;
  active_nodes: string;
  total_fees: string | null;
  compute_hours: string | null;
  transactions: string;
  core_hours: string | null;
  ram_hours: string | null;
  gpu_hours: string | null;
}

interface GpuGroupRow {
  group_name: string;
  value: string | null;
}

export async function getPlanStats(period: PlanPeriod): Promise<PlanStatsResponse> {
  const cutoff = getDataCutoff();
  const rangeStart = getRangeStart(cutoff, period);
  const granularity = getGranularity(period);

  const cutoffMs = toEpochMs(cutoff);
  const startMs = rangeStart ? toEpochMs(rangeStart) : null;

  // Build WHERE clause for time range
  const timeWhere = startMs
    ? 'stop_at <= $1 AND stop_at >= $2'
    : 'stop_at <= $1';
  const timeParams = startMs ? [cutoffMs, startMs] : [cutoffMs];

  // 1. Get totals
  const totalsQuery = `
    SELECT
      COUNT(DISTINCT node_id) as active_nodes,
      COALESCE(SUM(invoice_amount), 0) as total_fees,
      COALESCE(SUM((stop_at - start_at) / 1000.0 / 3600.0), 0) as compute_hours,
      COUNT(*) as transactions,
      COALESCE(SUM(cpu * (stop_at - start_at) / 1000.0 / 3600.0), 0) as core_hours,
      COALESCE(SUM(ram * (stop_at - start_at) / 1000.0 / 3600.0 / 1024.0), 0) as ram_hours,
      COALESCE(SUM(
        CASE WHEN gpu_class_id IS NOT NULL AND gpu_class_id != ''
        THEN (stop_at - start_at) / 1000.0 / 3600.0
        ELSE 0 END
      ), 0) as gpu_hours
    FROM node_plan
    WHERE ${timeWhere}
  `;

  const totalsResult = await query<TotalsRow>(totalsQuery, timeParams);
  const totalsRow = totalsResult[0];

  const totals: PlanTotals = {
    active_nodes: parseInt(totalsRow.active_nodes, 10) || 0,
    total_fees: parseFloat(totalsRow.total_fees || '0'),
    compute_hours: parseFloat(totalsRow.compute_hours || '0'),
    transactions: parseInt(totalsRow.transactions, 10) || 0,
    core_hours: parseFloat(totalsRow.core_hours || '0'),
    ram_hours: parseFloat(totalsRow.ram_hours || '0'),
    gpu_hours: parseFloat(totalsRow.gpu_hours || '0'),
  };

  // 2. Get time series
  const bucketInterval = granularity === 'hourly' ? 'hour' : 'day';
  const timeSeriesQuery = `
    SELECT
      date_trunc('${bucketInterval}', to_timestamp(stop_at / 1000.0)) as bucket,
      COUNT(DISTINCT node_id) as active_nodes,
      COALESCE(SUM(invoice_amount), 0) as total_fees,
      COALESCE(SUM((stop_at - start_at) / 1000.0 / 3600.0), 0) as compute_hours,
      COUNT(*) as transactions,
      COALESCE(SUM(cpu * (stop_at - start_at) / 1000.0 / 3600.0), 0) as core_hours,
      COALESCE(SUM(ram * (stop_at - start_at) / 1000.0 / 3600.0 / 1024.0), 0) as ram_hours,
      COALESCE(SUM(
        CASE WHEN gpu_class_id IS NOT NULL AND gpu_class_id != ''
        THEN (stop_at - start_at) / 1000.0 / 3600.0
        ELSE 0 END
      ), 0) as gpu_hours
    FROM node_plan
    WHERE ${timeWhere}
    GROUP BY bucket
    ORDER BY bucket
  `;

  const timeSeriesResult = await query<TimeSeriesRow>(timeSeriesQuery, timeParams);
  const timeSeries: PlanDataPoint[] = timeSeriesResult.map((row) => ({
    timestamp: row.bucket.toISOString(),
    active_nodes: parseInt(row.active_nodes, 10) || 0,
    total_fees: parseFloat(row.total_fees || '0'),
    compute_hours: parseFloat(row.compute_hours || '0'),
    transactions: parseInt(row.transactions, 10) || 0,
    core_hours: parseFloat(row.core_hours || '0'),
    ram_hours: parseFloat(row.ram_hours || '0'),
    gpu_hours: parseFloat(row.gpu_hours || '0'),
  }));

  // 3. Get GPU hours by model
  const gpuHoursByModelQuery = `
    SELECT
      COALESCE(gc.gpu_class_name, 'Unknown') as group_name,
      COALESCE(SUM((np.stop_at - np.start_at) / 1000.0 / 3600.0), 0) as value
    FROM node_plan np
    LEFT JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    WHERE ${timeWhere.replace(/\$/g, (m) => `np.stop_at <= $1${startMs ? ' AND np.stop_at >= $2' : ''}`.includes(m) ? m : m)}
      AND np.gpu_class_id IS NOT NULL AND np.gpu_class_id != ''
    GROUP BY gc.gpu_class_name
    ORDER BY value DESC
  `.replace(timeWhere, timeWhere.split('stop_at').join('np.stop_at'));

  const gpuHoursByModelResult = await query<GpuGroupRow>(gpuHoursByModelQuery, timeParams);
  const gpuHoursByModel: GroupedMetric[] = gpuHoursByModelResult.map((row) => ({
    group: row.group_name,
    value: parseFloat(row.value || '0'),
  }));

  // 4. Get GPU hours by VRAM
  const gpuHoursByVramQuery = `
    SELECT
      COALESCE(gc.vram_gb::text || ' GB', 'Unknown') as group_name,
      COALESCE(SUM((np.stop_at - np.start_at) / 1000.0 / 3600.0), 0) as value
    FROM node_plan np
    LEFT JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    WHERE np.stop_at <= $1 ${startMs ? 'AND np.stop_at >= $2' : ''}
      AND np.gpu_class_id IS NOT NULL AND np.gpu_class_id != ''
    GROUP BY gc.vram_gb
    ORDER BY gc.vram_gb
  `;

  const gpuHoursByVramResult = await query<GpuGroupRow>(gpuHoursByVramQuery, timeParams);
  const gpuHoursByVram: GroupedMetric[] = gpuHoursByVramResult.map((row) => ({
    group: row.group_name,
    value: parseFloat(row.value || '0'),
  }));

  // 5. Get active nodes by GPU model
  const activeNodesByModelQuery = `
    SELECT
      COALESCE(gc.gpu_class_name, 'No GPU') as group_name,
      COUNT(DISTINCT np.node_id)::text as value
    FROM node_plan np
    LEFT JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    WHERE np.stop_at <= $1 ${startMs ? 'AND np.stop_at >= $2' : ''}
    GROUP BY gc.gpu_class_name
    ORDER BY value DESC
  `;

  const activeNodesByModelResult = await query<GpuGroupRow>(activeNodesByModelQuery, timeParams);
  const activeNodesByGpuModel: GroupedMetric[] = activeNodesByModelResult.map((row) => ({
    group: row.group_name,
    value: parseFloat(row.value || '0'),
  }));

  // 6. Get active nodes by VRAM
  const activeNodesByVramQuery = `
    SELECT
      COALESCE(gc.vram_gb::text || ' GB', 'No GPU') as group_name,
      COUNT(DISTINCT np.node_id)::text as value
    FROM node_plan np
    LEFT JOIN gpu_classes gc ON np.gpu_class_id = gc.gpu_class_id
    WHERE np.stop_at <= $1 ${startMs ? 'AND np.stop_at >= $2' : ''}
    GROUP BY gc.vram_gb
    ORDER BY gc.vram_gb NULLS FIRST
  `;

  const activeNodesByVramResult = await query<GpuGroupRow>(activeNodesByVramQuery, timeParams);
  const activeNodesByVram: GroupedMetric[] = activeNodesByVramResult.map((row) => ({
    group: row.group_name,
    value: parseFloat(row.value || '0'),
  }));

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
  };
}
