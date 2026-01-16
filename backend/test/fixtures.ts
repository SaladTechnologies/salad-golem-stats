// Test fixtures for integration tests

// Helper to create timestamps relative to "now"
// DATA_OFFSET_HOURS = 48 in production, so we need to create data in that time window
const DATA_OFFSET_HOURS = 48;
const HOUR_MS = 3600000;

function getTestTimestamp(hoursFromCutoff: number): number {
  const cutoff = Date.now() - DATA_OFFSET_HOURS * HOUR_MS;
  return cutoff + hoursFromCutoff * HOUR_MS;
}

export const testGpuClasses = [
  { gpu_class_id: 'rtx4090', gpu_class_name: 'RTX 4090', vram_gb: 24 },
  { gpu_class_id: 'rtx3090', gpu_class_name: 'RTX 3090', vram_gb: 24 },
  { gpu_class_id: 'rtx3080', gpu_class_name: 'RTX 3080', vram_gb: 10 },
];

// Create node plans spread across the last 6 hours (relative to cutoff)
// This ensures they'll be included in the 6h stats
export const testNodePlans = [
  // GPU nodes (active in last 6 hours)
  {
    org_name: 'test-org',
    node_id: 'node-gpu-1',
    start_at: getTestTimestamp(-6), // Started 6 hours before cutoff
    stop_at: getTestTimestamp(-1), // Stopped 1 hour before cutoff
    invoice_amount: 5.0,
    usd_per_hour: 1.0,
    gpu_class_id: 'rtx4090',
    ram: 32768, // 32 GB in MB
    cpu: 16,
  },
  {
    org_name: 'test-org',
    node_id: 'node-gpu-2',
    start_at: getTestTimestamp(-5),
    stop_at: null, // Still running
    invoice_amount: 10.0,
    usd_per_hour: 2.0,
    gpu_class_id: 'rtx3090',
    ram: 65536, // 64 GB
    cpu: 32,
  },
  {
    org_name: 'test-org',
    node_id: 'node-gpu-3',
    start_at: getTestTimestamp(-4),
    stop_at: getTestTimestamp(-2),
    invoice_amount: 8.0,
    usd_per_hour: 4.0,
    gpu_class_id: 'rtx3080',
    ram: 16384, // 16 GB
    cpu: 8,
  },

  // CPU-only nodes (active in last 6 hours)
  {
    org_name: 'test-org',
    node_id: 'node-cpu-1',
    start_at: getTestTimestamp(-6),
    stop_at: getTestTimestamp(-3),
    invoice_amount: 3.0,
    usd_per_hour: 1.0,
    gpu_class_id: null,
    ram: 8192, // 8 GB
    cpu: 4,
  },
  {
    org_name: 'test-org',
    node_id: 'node-cpu-2',
    start_at: getTestTimestamp(-5),
    stop_at: null, // Still running
    invoice_amount: 5.0,
    usd_per_hour: 1.0,
    gpu_class_id: null,
    ram: 16384, // 16 GB
    cpu: 8,
  },

  // Historical nodes (for 30-day historical stats)
  {
    org_name: 'test-org',
    node_id: 'node-historical-1',
    start_at: getTestTimestamp(-24 * 15), // 15 days ago
    stop_at: getTestTimestamp(-24 * 14),
    invoice_amount: 100.0,
    usd_per_hour: 4.17,
    gpu_class_id: 'rtx4090',
    ram: 32768,
    cpu: 16,
  },
  {
    org_name: 'test-org',
    node_id: 'node-historical-2',
    start_at: getTestTimestamp(-24 * 10), // 10 days ago
    stop_at: getTestTimestamp(-24 * 9),
    invoice_amount: 50.0,
    usd_per_hour: 2.08,
    gpu_class_id: 'rtx3090',
    ram: 65536,
    cpu: 32,
  },
];

// Create utilization test data - nodes that were active at specific times
// This helps test the 30-second granularity utilization endpoint
export function createUtilizationTestData(): typeof testNodePlans {
  const plans = [];
  const numNodes = 10;

  for (let i = 0; i < numNodes; i++) {
    plans.push({
      org_name: 'test-org',
      node_id: `utilization-node-${i}`,
      start_at: getTestTimestamp(-6 + i * 0.5), // Stagger starts
      stop_at: i % 2 === 0 ? getTestTimestamp(-1) : null, // Half still running
      invoice_amount: 5.0,
      usd_per_hour: 1.0,
      gpu_class_id: i % 3 === 0 ? 'rtx4090' : null,
      ram: 16384,
      cpu: 8,
    });
  }

  return plans;
}
