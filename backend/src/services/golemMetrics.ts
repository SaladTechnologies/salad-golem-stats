import { getPlanStats } from './planMetrics.js';
import { query } from '../db/connection.js';
import { config } from '../config.js';

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

  // Calculate total resources from current active nodes
  // Query the database for current resource totals
  const resourcesResult = await query<{
    total_cores: string;
    total_ram_gib: string;
    total_disk_gib: string;
    total_gpus: string;
  }>(
    `
    SELECT
      COALESCE(SUM(cpu), 0) as total_cores,
      COALESCE(SUM(ram / 1024.0), 0) as total_ram_gib,
      COALESCE(SUM(disk / 1024.0), 0) as total_disk_gib,
      COALESCE(COUNT(DISTINCT CASE WHEN gpu_class_id IS NOT NULL AND gpu_class_id != '' THEN node_id END), 0) as total_gpus
    FROM node_plan
    WHERE stop_at IS NULL OR stop_at > EXTRACT(EPOCH FROM NOW()) * 1000
  `,
    []
  );

  const resources = resourcesResult[0] || {
    total_cores: '0',
    total_ram_gib: '0',
    total_disk_gib: '0',
    total_gpus: '0',
  };

  // For "computing" providers, we'll estimate based on recent activity
  // Use nodes that have had activity in the last hour
  const computingResult = await query<{ computing_nodes: string }>(
    `
    SELECT COUNT(DISTINCT node_id) as computing_nodes
    FROM node_plan
    WHERE start_at > (EXTRACT(EPOCH FROM NOW()) * 1000 - 3600000)
      OR (stop_at IS NULL OR stop_at > (EXTRACT(EPOCH FROM NOW()) * 1000 - 3600000))
  `,
    []
  );

  const computingNodes = parseInt(computingResult[0]?.computing_nodes || '0', 10);

  // Version data - for now, we'll use a placeholder since we don't have version info
  // This would need to be extended based on actual version tracking in the database
  const versions = [
    {
      version: '1.0.0',
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
      disk_gib: parseFloat(resources.total_disk_gib),
      gpus: parseInt(resources.total_gpus, 10),
    },
    earnings: {
      '6h': stats6h.totals.total_fees,
      '24h': stats24h.totals.total_fees,
      '168h': stats7d.totals.total_fees,
      '720h': stats30d.totals.total_fees,
      '2160h': stats90d.totals.total_fees,
      total: statsTotal.totals.total_fees,
    },
    versions,
  };
}

export async function getGolemHistoricalStats(): Promise<HistoricalStatsResponse> {
  // Get 30 days of historical data
  const stats30d = await getPlanStats('30d');

  // Query for historical resource data by runtime (VM vs VM-NVIDIA)
  // Get hourly data for last 24 hours, daily for older
  const networkStatsResult = await query<{
    bucket: Date;
    has_gpu: boolean;
    online: string;
    cores: string;
    ram_gib: string;
    disk_gib: string;
    gpus: string;
  }>(
    `
    WITH time_buckets AS (
      SELECT
        CASE
          WHEN bucket >= NOW() - INTERVAL '24 hours' THEN date_trunc('hour', bucket)
          ELSE date_trunc('day', bucket)
        END as bucket,
        CASE WHEN gpu_class_id IS NOT NULL AND gpu_class_id != '' THEN true ELSE false END as has_gpu,
        COUNT(DISTINCT node_id) as online,
        SUM(cpu) as cores,
        SUM(ram / 1024.0) as ram_gib,
        SUM(disk / 1024.0) as disk_gib,
        COUNT(DISTINCT CASE WHEN gpu_class_id IS NOT NULL AND gpu_class_id != '' THEN node_id END) as gpus
      FROM (
        SELECT DISTINCT ON (date_trunc('hour', to_timestamp(start_at / 1000.0)), node_id)
          date_trunc('hour', to_timestamp(start_at / 1000.0)) as bucket,
          node_id,
          cpu,
          ram,
          disk,
          gpu_class_id
        FROM node_plan
        WHERE start_at >= EXTRACT(EPOCH FROM (NOW() - INTERVAL '30 days')) * 1000
        ORDER BY date_trunc('hour', to_timestamp(start_at / 1000.0)), node_id, start_at DESC
      ) x
      GROUP BY 1, 2
    )
    SELECT
      bucket,
      has_gpu,
      online::text,
      cores::text,
      ram_gib::text,
      disk_gib::text,
      gpus::text
    FROM time_buckets
    WHERE bucket >= NOW() - INTERVAL '30 days'
    ORDER BY bucket, has_gpu
  `,
    []
  );

  // Separate VM and VM-NVIDIA data
  const vmData: HistoricalDataPoint[] = [];
  const vmNvidiaData: HistoricalDataPoint[] = [];

  for (const row of networkStatsResult) {
    const dataPoint: HistoricalDataPoint = {
      date: Math.floor(row.bucket.getTime() / 1000),
      online: parseInt(row.online, 10),
      cores: parseInt(row.cores, 10),
      memory_gib: parseFloat(row.ram_gib),
      disk_gib: parseFloat(row.disk_gib),
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
  const utilizationResult = await query<{
    bucket: Date;
    computing_nodes: string;
  }>(
    `
    WITH time_buckets AS (
      SELECT generate_series(
        NOW() - INTERVAL '${utilizationHours} hours',
        NOW(),
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
    []
  );

  const utilization: Array<[number, number]> = utilizationResult.map((row) => [
    Math.floor(row.bucket.getTime() / 1000),
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
      date: new Date(point.timestamp).toISOString().split('T')[0],
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
