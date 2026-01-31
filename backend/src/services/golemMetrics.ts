import { getPlanStats } from './networkMetrics.js';
import { query } from '../db/connection.js';
import { config } from '../config.js';

// Can't return data that hasn't gone through Golem yet
const DATA_OFFSET_HOURS = 48;

interface NetworkStatsResponse {
  timestamp: string;
  network_id: string;
  providers: {
    online: number;
    computing: number;
  };
  resources: {
    cores: number;
    memory_gib: number;
    disk_gib: number;
    gpus: number;
  };
  earnings: {
    '6h': number;
    '24h': number;
    '168h': number;
    '720h': number;
    '2160h': number;
    total: number;
  };
  versions: Array<{
    version: string;
    count: number;
    rc: boolean;
  }>;
}

interface HistoricalDataPoint {
  date: number;
  online: number;
  cores: number;
  memory_gib: number;
  disk_gib: number;
  gpus: number;
}

interface HistoricalStatsResponse {
  network_id: string;
  network_stats: {
    vm: HistoricalDataPoint[];
    'vm-nvidia': HistoricalDataPoint[];
  };
  utilization: Array<[number, number]>;
  computing_daily: Array<{
    date: string;
    total: number;
  }>;
}

export async function getGolemNetworkStats(): Promise<NetworkStatsResponse> {
  // Get current stats from various time periods
  const [stats6h, stats24h, stats7d, stats30d, stats90d, statsTotal] = await Promise.all([
    getPlanStats('6h'),
    getPlanStats('24h'),
    getPlanStats('7d'),
    getPlanStats('30d'),
    getPlanStats('90d'),
    getPlanStats('total'),
  ]);

  // Get current online providers and computing providers
  // We'll use the 6h stats as the most recent snapshot
  const currentData = stats6h.time_series[stats6h.time_series.length - 1] || {
    active_nodes: 0,
    total_fees: 0,
    compute_hours: 0,
    core_hours: 0,
    ram_hours: 0,
    gpu_hours: 0,
  };

  const cutoffMs = Date.now() - (DATA_OFFSET_HOURS * 3600000);;
  const startMs = cutoffMs - (6 * 3600000);

  const resourcesResult = await query<{
    total_cores: string;
    total_ram_gib: string;
    total_gpus: string;
  }>(
    `
    SELECT
      COALESCE(SUM(cpu), 0) as total_cores,
      COALESCE(SUM(ram / 1024.0), 0) as total_ram_gib,
      COALESCE(COUNT(DISTINCT CASE WHEN gpu_class_id IS NOT NULL AND gpu_class_id != '' THEN node_id END), 0) as total_gpus
    FROM (
      SELECT DISTINCT ON (node_id) node_id, cpu, ram, gpu_class_id
      FROM node_plan
      WHERE start_at < $1 AND (stop_at IS NULL OR stop_at >= $2)
      ORDER BY node_id, start_at DESC
    ) latest_nodes
  `,
    [cutoffMs, startMs]
  );

  const resources = resourcesResult[0] || {
    total_cores: '0',
    total_ram_gib: '0',
    total_gpus: '0',
  };

  // For "computing" providers, use the same as online
  // In our system, online nodes are computing nodes
  const computingNodes = currentData.active_nodes;

  // Version data - for now, we'll use a placeholder since we don't have version info
  // This would need to be extended based on actual version tracking in the database
  const versions = [
    {
      version: '0.17.6',
      count: currentData.active_nodes,
      rc: false,
    },
  ];

  return {
    timestamp: new Date().toISOString(),
    network_id: 'salad',
    providers: {
      online: currentData.active_nodes,
      computing: computingNodes,
    },
    resources: {
      cores: parseInt(resources.total_cores, 10),
      memory_gib: parseFloat(resources.total_ram_gib),
      disk_gib: 0, // Disk is not tracked in our database
      gpus: parseInt(resources.total_gpus, 10),
    },
    earnings: {
      '6h': stats6h.totals.observed_fees,
      '24h': stats24h.totals.observed_fees,
      '168h': stats7d.totals.observed_fees,
      '720h': stats30d.totals.observed_fees,
      '2160h': stats90d.totals.observed_fees,
      total: statsTotal.totals.observed_fees,
    },
    versions,
  };
}

export async function getGolemHistoricalStats(): Promise<HistoricalStatsResponse> {
  // Get 30 days of historical data
  const stats30d = await getPlanStats('30d');

  // Query for historical resource data by runtime (VM vs VM-NVIDIA)
  // Get hourly data for last 24 hours, daily for older
  const historicalCutoff = Date.now() - (DATA_OFFSET_HOURS * 3600000);
  const historicalStart = historicalCutoff - (30 * 24 * 3600000); // 30 days

  const networkStatsResult = await query<{
    bucket: Date;
    has_gpu: boolean;
    online: string;
    cores: string;
    ram_gib: string;
    gpus: string;
  }>(
    `
    WITH hourly_timestamps AS (
      -- Generate hourly timestamps for the last 24 hours
      SELECT generate_series(
        date_trunc('hour', to_timestamp($1 / 1000.0) - INTERVAL '23 hours'),
        date_trunc('hour', to_timestamp($1 / 1000.0)),
        INTERVAL '1 hour'
      ) as timestamp
    ),
    daily_timestamps AS (
      -- Generate daily timestamps (midnight UTC) for the older data, stopping before the hourly data starts
      SELECT generate_series(
        date_trunc('day', to_timestamp($2 / 1000.0)),
        date_trunc('day', to_timestamp($1 / 1000.0)) - INTERVAL '24 hours',
        INTERVAL '1 day'
      ) as timestamp
    ),
    all_timestamps AS (
      SELECT timestamp FROM hourly_timestamps
      UNION
      SELECT timestamp FROM daily_timestamps
    ),
    nodes_at_timestamp AS (
      -- For each timestamp, find the plan for each node active at that point in time
      SELECT
        t.timestamp,
        np.node_id,
        np.cpu,
        np.ram,
        CASE WHEN np.gpu_class_id IS NOT NULL AND np.gpu_class_id != '' THEN true ELSE false END as has_gpu
      FROM all_timestamps t
      JOIN node_plan np ON 
        np.start_at < (EXTRACT(EPOCH FROM (t.timestamp + INTERVAL '5 minutes')) * 1000) 
        AND (np.stop_at IS NULL OR np.stop_at > (EXTRACT(EPOCH FROM t.timestamp) * 1000))
    )
    SELECT
      timestamp as bucket,
      has_gpu,
      COUNT(node_id)::text as online,
      SUM(cpu)::text as cores,
      SUM(ram / 1024.0)::text as ram_gib,
      COUNT(CASE WHEN has_gpu THEN 1 END)::text as gpus
    FROM nodes_at_timestamp
    GROUP BY timestamp, has_gpu
    ORDER BY timestamp, has_gpu
  `,
    [historicalCutoff, historicalStart]
  );

  // Separate VM and VM-NVIDIA data
  const vmData: HistoricalDataPoint[] = [];
  const vmNvidiaData: HistoricalDataPoint[] = [];

  for (const row of networkStatsResult) {
    const dataPoint: HistoricalDataPoint = {
      date: Math.floor((row.bucket.getTime() + DATA_OFFSET_HOURS * 3600 * 1000) / 1000),
      online: parseInt(row.online, 10),
      cores: parseInt(row.cores, 10),
      memory_gib: parseFloat(row.ram_gib),
      disk_gib: 0, // Disk is not tracked in our database
      gpus: parseInt(row.gpus, 10),
    };

    if (row.has_gpu) {
      vmNvidiaData.push(dataPoint);
    } else {
      vmData.push(dataPoint);
    }
  }

  // Get utilization data at configured granularity (default: 30-second intervals for last 6 hours)

  const granularitySeconds = config.golemUtilizationGranularitySeconds;
  const utilizationHours = 6;
  const utilizationCutoff = Date.now() - (DATA_OFFSET_HOURS * 3600000);
  const utilizationStart = utilizationCutoff - (utilizationHours * 3600000);
  // Subtract one interval to make end exclusive (generate_series includes both endpoints)
  const utilizationEnd = utilizationCutoff - (granularitySeconds * 1000);

  const utilizationResult = await query<{
    bucket: Date;
    computing_nodes: string;
  }>(
    `
    WITH time_buckets AS (
      SELECT generate_series(
        to_timestamp($1 / 1000.0),
        to_timestamp($2 / 1000.0),
        INTERVAL '${granularitySeconds} seconds'
      ) as bucket
    )
    SELECT
      b.bucket,
      COUNT(DISTINCT np.node_id)::text as computing_nodes
    FROM time_buckets b
    LEFT JOIN node_plan np ON
      np.start_at < (EXTRACT(EPOCH FROM (b.bucket + INTERVAL '${granularitySeconds} seconds')) * 1000)
      AND (np.stop_at IS NULL OR np.stop_at >= (EXTRACT(EPOCH FROM b.bucket) * 1000))
    GROUP BY b.bucket
    ORDER BY b.bucket
  `,
    [utilizationStart, utilizationEnd]
  );

  const utilization: Array<[number, number]> = utilizationResult.map((row) => [
    Math.floor((row.bucket.getTime() + DATA_OFFSET_HOURS * 3600 * 1000) / 1000),
    parseInt(row.computing_nodes, 10),
  ]);

  // Get daily computing totals
  const computingDaily = stats30d.time_series
    .filter((point) => {
      // Only include daily data points
      const date = new Date(point.timestamp);
      return date.getUTCHours() === 0;
    })
    .map((point) => ({
      date: new Date(new Date(point.timestamp).getTime()).toISOString().split('T')[0],
      total: point.active_nodes,
    }));

  return {
    network_id: 'salad',
    network_stats: {
      vm: vmData,
      'vm-nvidia': vmNvidiaData,
    },
    utilization,
    computing_daily: computingDaily,
  };
}
