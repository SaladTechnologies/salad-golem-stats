-- Migration: Create tables for hourly and daily stats
-- DROP TABLE IF EXISTS hourly_distinct_counts;
-- DROP TABLE IF EXISTS daily_distinct_counts;
-- DROP TABLE IF EXISTS hourly_gpu_stats;


CREATE TABLE IF NOT EXISTS hourly_gpu_stats (
    id SERIAL PRIMARY KEY,
    hour TIMESTAMP NOT NULL,
    gpu_group TEXT NOT NULL,
    total_time_seconds DOUBLE PRECISION,
    total_invoice_amount DOUBLE PRECISION,
    total_ram_hours DOUBLE PRECISION,
    total_cpu_hours DOUBLE PRECISION,
    total_transaction_count INTEGER
);

CREATE TABLE IF NOT EXISTS hourly_distinct_counts (
    id SERIAL PRIMARY KEY,
    hour TIMESTAMP NOT NULL,
    gpu_group TEXT NOT NULL,
    unique_node_count INTEGER,
    unique_node_ram DOUBLE PRECISION,
    unique_node_cpu DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS daily_distinct_counts (
    id SERIAL PRIMARY KEY,
    day DATE NOT NULL,
    gpu_group TEXT NOT NULL,
    unique_node_count INTEGER,
    unique_node_ram DOUBLE PRECISION,
    unique_node_cpu DOUBLE PRECISION
);

-- Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_hourly_gpu_stats_hour ON hourly_gpu_stats(hour);
CREATE INDEX IF NOT EXISTS idx_hourly_distinct_counts_hour ON hourly_distinct_counts(hour);
CREATE INDEX IF NOT EXISTS idx_daily_distinct_counts_day ON daily_distinct_counts(day);
