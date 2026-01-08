-- ===============================================
-- 001_init.sql
-- Complete database schema for metrics dashboard
-- ===============================================

-- Core reference tables
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

-- Geographic data
CREATE TABLE IF NOT EXISTS city_snapshots (
    ts TIMESTAMP NOT NULL,
    name TEXT NOT NULL,
    count INTEGER NOT NULL,
    lat FLOAT NOT NULL,
    long FLOAT NOT NULL,
    PRIMARY KEY (ts, name)
);

-- Demo/test data
CREATE TABLE IF NOT EXISTS placeholder_transactions (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    provider_wallet TEXT NOT NULL,
    requester_wallet TEXT NOT NULL,
    tx TEXT NOT NULL,
    gpu TEXT,
    ram INTEGER,
    vcpus INTEGER,
    duration TEXT,
    invoiced_glm DOUBLE PRECISION,
    invoiced_dollar DOUBLE PRECISION
);

-- Import tracking
CREATE TABLE IF NOT EXISTS json_import_file (
    id SERIAL PRIMARY KEY,
    file_name TEXT UNIQUE NOT NULL
);

-- Main metrics data
CREATE TABLE IF NOT EXISTS node_plan (
    id SERIAL PRIMARY KEY,
    org_name TEXT,
    node_id TEXT,
    json_import_file_id INTEGER REFERENCES json_import_file(id),
    start_at BIGINT,
    stop_at BIGINT,
    invoice_amount DOUBLE PRECISION,
    usd_per_hour DOUBLE PRECISION,
    gpu_class_id TEXT,
    ram DOUBLE PRECISION,
    cpu DOUBLE PRECISION
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_gpu_classes_id ON gpu_classes(gpu_class_id);
CREATE INDEX IF NOT EXISTS idx_placeholder_transactions_ts ON placeholder_transactions(ts DESC);
CREATE INDEX IF NOT EXISTS idx_node_plan_org ON node_plan(org_name);
CREATE INDEX IF NOT EXISTS idx_node_plan_node_id ON node_plan(node_id);
CREATE INDEX IF NOT EXISTS idx_node_plan_gpu_class ON node_plan(gpu_class_id);
CREATE INDEX IF NOT EXISTS idx_node_plan_stop_at ON node_plan(stop_at);
CREATE INDEX IF NOT EXISTS idx_node_plan_start_at ON node_plan(start_at);
CREATE INDEX IF NOT EXISTS idx_node_plan_time_range ON node_plan(stop_at, start_at);
CREATE INDEX IF NOT EXISTS idx_node_plan_gpu_time ON node_plan(gpu_class_id, stop_at, start_at)
WHERE gpu_class_id IS NOT NULL AND gpu_class_id != '';

