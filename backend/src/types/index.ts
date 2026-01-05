export type Period = 'day' | 'week' | 'two_weeks' | 'month';

export type Metric =
  | 'total_time_seconds'
  | 'total_time_hours'
  | 'total_invoice_amount'
  | 'total_ram_hours'
  | 'total_cpu_hours'
  | 'total_transaction_count'
  | 'unique_node_count'
  | 'unique_node_ram'
  | 'unique_node_cpu';

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

export interface CityCount {
  city: string;
  count: number;
  lat: number;
  lon: number;
}

export interface Transaction {
  ts: string;
  provider_wallet: string;
  requester_wallet: string;
  tx: string;
  gpu: string;
  ram: number;
  vcpus: number;
  duration: string;
  invoiced_glm: number;
  invoiced_dollar: number;
}

export interface TransactionsResponse {
  transactions: Transaction[];
  next_cursor: string | null;
  prev_cursor: string | null;
  total: number;
}

export interface GpuStatsResponse {
  [metric: string]: MetricsByGroup;
}
