-- ===============================================
-- 003_overlap_query_indices.sql
-- Add indices for efficient overlap/range queries
-- Supports queries like: start_at < $1 AND stop_at >= $2
-- ===============================================

-- Index on start_at for overlap queries
CREATE INDEX IF NOT EXISTS idx_node_plan_start_at ON node_plan(start_at);

-- Composite index for time range queries (most selective column first)
-- Helps with: WHERE stop_at >= $start AND stop_at <= $end AND start_at < $end
CREATE INDEX IF NOT EXISTS idx_node_plan_time_range ON node_plan(stop_at, start_at);

-- Composite index for GPU time series queries
-- Helps with: WHERE gpu_class_id IS NOT NULL AND start_at < $1 AND stop_at >= $2
CREATE INDEX IF NOT EXISTS idx_node_plan_gpu_time ON node_plan(gpu_class_id, stop_at, start_at)
WHERE gpu_class_id IS NOT NULL AND gpu_class_id != '';
