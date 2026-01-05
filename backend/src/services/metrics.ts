import { query } from '../db/connection.js';
import { gpuClassNames } from './gpuClasses.js';
import { getQueryParameters } from './queryParams.js';
import { Metric, Period, DataPoint, MetricsByGroup } from '../types/index.js';

interface MetricRow {
  gpu_group: string;
  ts: Date;
  value: number;
}

export async function getMetrics(
  metric: Metric,
  period: Period,
  gpu: string
): Promise<DataPoint[]> {
  const { since, table, tsCol } = getQueryParameters(metric, period);

  let sql: string;
  if (table === 'hourly_gpu_stats' && tsCol === 'day') {
    sql = `
      SELECT DATE(hour) as ts, SUM(${metric}) as value
      FROM ${table}
      WHERE gpu_group = $1 AND hour >= $2
      GROUP BY DATE(hour)
      ORDER BY ts ASC
    `;
  } else {
    sql = `
      SELECT ${tsCol} as ts, ${metric} as value
      FROM ${table}
      WHERE gpu_group = $1 AND ${tsCol} >= $2
      ORDER BY ${tsCol} ASC
    `;
  }

  const rows = await query<{ ts: Date; value: number }>(sql, [gpu, since]);
  return rows.map((r) => ({ x: r.ts, y: r.value ?? 0 }));
}

export async function getMetricsByGpu(
  metric: Metric,
  period: Period,
  groupBy: 'gpu' | 'vram' = 'gpu'
): Promise<MetricsByGroup> {
  const { since, table, tsCol } = getQueryParameters(metric, period);

  let sql: string;
  if (table === 'hourly_gpu_stats' && tsCol === 'day') {
    sql = `
      SELECT gpu_group, DATE(hour) as ts, SUM(${metric}) as value
      FROM ${table}
      WHERE gpu_group NOT IN ('all', 'any_gpu', 'no_gpu') AND hour >= $1
      GROUP BY gpu_group, DATE(hour)
      ORDER BY gpu_group, ts ASC
    `;
  } else {
    sql = `
      SELECT gpu_group, ${tsCol} as ts, ${metric} as value
      FROM ${table}
      WHERE gpu_group NOT IN ('all', 'any_gpu', 'no_gpu') AND ${tsCol} >= $1
      ORDER BY ${tsCol} ASC
    `;
  }

  const rows = await query<MetricRow>(sql, [since]);

  // Extract unique sorted timestamps for labels
  const labelsSet = new Set<string>();
  for (const r of rows) {
    labelsSet.add(r.ts.toISOString());
  }
  const labels = Array.from(labelsSet).sort();
  const labelIndex = new Map(labels.map((ts, i) => [ts, i]));

  if (groupBy === 'vram') {
    return aggregateByVram(rows, labels, labelIndex);
  } else {
    return aggregateByGpu(rows, labels, labelIndex);
  }
}

function aggregateByVram(
  rows: MetricRow[],
  labels: string[],
  labelIndex: Map<string, number>
): MetricsByGroup {
  // Build a mapping from gpu_group to VRAM
  const gpuToVram = new Map<string, string>();
  for (const [gpuId, name] of gpuClassNames) {
    const match = name.match(/\((\d+\s*[gG][bB])\)/);
    const vram = match ? match[1].replace(/\s/g, '') : 'Unknown';
    gpuToVram.set(gpuId, vram);
  }

  // Aggregate by VRAM
  const vramGroupSums = new Map<string, number>();
  const vramEntries = new Map<string, Map<string, number>>();

  for (const r of rows) {
    const vram = gpuToVram.get(r.gpu_group) ?? 'Unknown';
    const tsKey = r.ts.toISOString();
    const value = r.value ?? 0;

    vramGroupSums.set(vram, (vramGroupSums.get(vram) ?? 0) + value);

    if (!vramEntries.has(vram)) {
      vramEntries.set(vram, new Map());
    }
    const entries = vramEntries.get(vram)!;
    entries.set(tsKey, (entries.get(tsKey) ?? 0) + value);
  }

  // Find top 5 VRAM groups
  const sortedVram = Array.from(vramGroupSums.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);
  const topVramNames = new Set(sortedVram.map(([v]) => v));

  const datasets: { label: string; data: number[] }[] = [];

  // Prepare data arrays aligned to labels
  for (const vram of topVramNames) {
    const entries = vramEntries.get(vram)!;
    const data = labels.map((ts) => entries.get(ts) ?? 0);
    datasets.push({ label: vram, data });
  }

  // Aggregate all other VRAMs into 'Other'
  const otherData = new Array(labels.length).fill(0);
  for (const [vram, entries] of vramEntries) {
    if (!topVramNames.has(vram)) {
      for (const [ts, value] of entries) {
        const idx = labelIndex.get(ts);
        if (idx !== undefined) {
          otherData[idx] += value;
        }
      }
    }
  }
  if (otherData.some((v) => v > 0)) {
    datasets.push({ label: 'Other', data: otherData });
  }

  return { labels, datasets };
}

function aggregateByGpu(
  rows: MetricRow[],
  labels: string[],
  labelIndex: Map<string, number>
): MetricsByGroup {
  // Calculate sums per GPU group
  const groupSums = new Map<string, number>();
  for (const r of rows) {
    groupSums.set(r.gpu_group, (groupSums.get(r.gpu_group) ?? 0) + (r.value ?? 0));
  }

  // Find top 5 GPU groups
  const topGroups = Array.from(groupSums.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);
  const topGroupNames = new Set(topGroups.map(([g]) => g));

  // Prepare group_entries and other_entries as arrays aligned to labels
  const groupEntries = new Map<string, number[]>();
  for (const group of topGroupNames) {
    groupEntries.set(group, new Array(labels.length).fill(0));
  }
  const otherEntries = new Array(labels.length).fill(0);

  for (const r of rows) {
    const tsKey = r.ts.toISOString();
    const idx = labelIndex.get(tsKey);
    if (idx === undefined) continue;

    const value = r.value ?? 0;
    if (topGroupNames.has(r.gpu_group)) {
      groupEntries.get(r.gpu_group)![idx] += value;
    } else {
      otherEntries[idx] += value;
    }
  }

  const datasets: { label: string; data: number[] }[] = [];

  // Only send the readable name as the group
  for (const gpuGroup of topGroupNames) {
    let label = gpuClassNames.get(gpuGroup);
    if (!label) {
      console.warn(`[WARNING] gpu_group '${gpuGroup}' not found in gpu_class_names, using fallback.`);
      label = gpuGroup;
    }
    datasets.push({ label, data: groupEntries.get(gpuGroup)! });
  }

  if (otherEntries.some((v) => v > 0)) {
    datasets.push({ label: 'Other', data: otherEntries });
  }

  return { labels, datasets };
}
