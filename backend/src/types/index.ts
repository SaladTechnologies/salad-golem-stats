export type Period = "day" | "week" | "two_weeks" | "month";

export type Metric =
  | "total_time_seconds"
  | "total_time_hours"
  | "total_invoice_amount"
  | "total_ram_hours"
  | "total_cpu_hours"
  | "total_transaction_count"
  | "unique_node_count"
  | "unique_node_ram"
  | "unique_node_cpu";

export interface QueryParams {
  since: Date;
  table: string;
  tsCol: string;
}

export interface DataPoint {
  x: Date | string;
  y: number;
}

export interface Dataset {
  label: string;
  data: number[];
}

export interface MetricsByGroup {
  labels: (Date | string)[];
  datasets: Dataset[];
}

export interface StatsResponse {
  total_time_hours: number;
  total_invoice_amount: number;
  unique_node_count: number;
  total_transaction_count: number;
}

export interface TrendsResponse {
  total_time_hours: DataPoint[];
  total_invoice_amount: DataPoint[];
  observed_fees: DataPoint[];
  transaction_count: DataPoint[];
  total_ram_hours: DataPoint[];
  total_cpu_hours: DataPoint[];
  total_transaction_count: DataPoint[];
  unique_node_count: DataPoint[];
  gpu_unique_node_count: MetricsByGroup;
  gpu_total_time_hours: MetricsByGroup;
  vram_unique_node_count: MetricsByGroup;
  vram_total_time_hours: MetricsByGroup;
}

export interface GeoPoint {
  lat: number;
  lng: number;
  normalized: number;
}

export interface Transaction {
  tx_hash: string;
  block_number: number;
  block_timestamp: string;
  from_address: string;
  to_address: string;
  value_glm: number;
  tx_type: string;
}

export interface TransactionsResponse {
  transactions: Transaction[];
  next_cursor: string | null;
  prev_cursor: string | null;
  total: number;
}

// Plan metrics types (for Golem integration)
// Predefined hours: 6, 24, 168 (7d), 720 (30d), 2160 (90d), or 'total'
export type PlanPeriod = "6h" | "24h" | "7d" | "30d" | "90d" | "total";

// Granularity for time series data
export type Granularity = "hourly" | "daily";

// Aggregated totals for a time range
export interface PlanTotals {
  active_nodes: number;
  total_fees: number;
  expected_fees: number; // Fees from plan data (existing total_fees)
  observed_fees: number; // Fees from transaction data
  transaction_count: number; // Count of GLM transactions
  compute_hours: number;
  core_hours: number;
  ram_hours: number;
  gpu_hours: number;
}

// Time series data point for plans
export interface PlanDataPoint {
  timestamp: string;
  active_nodes: number;
  total_fees: number;
  expected_fees: number; // Fees from plan data (existing total_fees) 
  observed_fees: number; // Fees from transaction data
  transaction_count: number; // Count of GLM transactions
  compute_hours: number;
  core_hours: number;
  ram_hours: number;
  gpu_hours: number;
}

// Breakdown by GPU model or VRAM
export interface GroupedMetric {
  group: string;
  value: number;
}

// Time series grouped by category (for stacked charts)
export interface GroupedTimeSeries {
  labels: string[];
  datasets: { label: string; data: number[] }[];
}

// Full response for plan stats endpoint
export interface PlanStatsResponse {
  period: PlanPeriod;
  granularity: Granularity;
  data_cutoff: string; // ISO timestamp - data only available up to this point
  range: {
    start: string;
    end: string;
  };
  totals: PlanTotals;
  // Breakdowns by GPU (aggregate totals)
  gpu_hours_by_model: GroupedMetric[];
  gpu_hours_by_vram: GroupedMetric[];
  active_nodes_by_gpu_model: GroupedMetric[];
  active_nodes_by_vram: GroupedMetric[];
  // Time series (hourly or daily based on range)
  time_series: PlanDataPoint[];
  // Time series grouped by GPU model/VRAM (for stacked charts)
  gpu_hours_by_model_ts: GroupedTimeSeries;
  gpu_hours_by_vram_ts: GroupedTimeSeries;
  active_nodes_by_gpu_model_ts: GroupedTimeSeries;
  active_nodes_by_vram_ts: GroupedTimeSeries;
}
