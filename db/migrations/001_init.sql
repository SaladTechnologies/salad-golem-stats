-- ===============================================
-- 001_init.sql
-- Initial database schema for metrics dashboard
-- ===============================================

CREATE TABLE IF NOT EXISTS hourly_gpu_stats (
    id SERIAL PRIMARY KEY,
    hour TIMESTAMP NOT NULL,
    gpu_group TEXT NOT NULL,
    total_time_seconds DOUBLE PRECISION,
    total_time_hours DOUBLE PRECISION,
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


CREATE TABLE IF NOT EXISTS gpu_classes (
    gpu_class_id TEXT PRIMARY KEY,
    batch_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    medium_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    gpu_type TEXT,
    gpu_class_name TEXT NOT NULL,
    vram_gb INTEGER
);

CREATE TABLE IF NOT EXISTS city_snapshots (
    ts TIMESTAMP NOT NULL,
    name TEXT NOT NULL,
    count INTEGER NOT NULL,
    lat FLOAT NOT NULL,
    long FLOAT NOT NULL,
    PRIMARY KEY (ts, name)
);

CREATE TABLE IF NOT EXISTS country_snapshots (
    ts TIMESTAMP NOT NULL,
    name TEXT NOT NULL,
    count INTEGER NOT NULL,
    lat FLOAT NOT NULL,
    long FLOAT NOT NULL,
    PRIMARY KEY (ts, name)
);

-- Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_hourly_gpu_stats_hour ON hourly_gpu_stats(hour);
CREATE INDEX IF NOT EXISTS idx_hourly_distinct_counts_hour ON hourly_distinct_counts(hour);
CREATE INDEX IF NOT EXISTS idx_daily_distinct_counts_day ON daily_distinct_counts(day);
CREATE INDEX IF NOT EXISTS idx_gpu_classes_id ON gpu_classes(gpu_class_id);

