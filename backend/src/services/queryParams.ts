import { Metric, Period, QueryParams } from '../types/index.js';

export function getQueryParameters(metric: Metric, period: Period): QueryParams {
  const now = new Date();
  let table: string;
  let tsCol: string;

  if (
    [
      'total_time_seconds',
      'total_time_hours',
      'total_invoice_amount',
      'total_ram_hours',
      'total_cpu_hours',
      'total_transaction_count',
    ].includes(metric)
  ) {
    table = 'hourly_gpu_stats';
    tsCol = period === 'day' ? 'hour' : 'day';
  } else if (['unique_node_count', 'unique_node_ram', 'unique_node_cpu'].includes(metric)) {
    if (period === 'day') {
      table = 'hourly_distinct_counts';
      tsCol = 'hour';
    } else {
      table = 'daily_distinct_counts';
      tsCol = 'day';
    }
  } else {
    throw new Error(`Unknown metric: ${metric}`);
  }

  let since: Date;
  switch (period) {
    case 'day':
      since = new Date(now.getTime() - 24 * 60 * 60 * 1000);
      break;
    case 'week':
      since = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      break;
    case 'two_weeks':
      since = new Date(now.getTime() - 14 * 24 * 60 * 60 * 1000);
      break;
    case 'month':
      since = new Date(now.getTime() - 31 * 24 * 60 * 60 * 1000);
      break;
  }

  if (tsCol === 'day') {
    since.setUTCHours(0, 0, 0, 0);
  }

  return { since, table, tsCol };
}
