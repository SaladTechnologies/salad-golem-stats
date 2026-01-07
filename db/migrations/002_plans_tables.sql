-- ===============================================
-- 002_plans_tables.sql
-- Import tables from SQLite plans.db
-- ===============================================

CREATE TABLE IF NOT EXISTS json_import_file (
    id SERIAL PRIMARY KEY,
    file_name TEXT UNIQUE NOT NULL
);

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

CREATE TABLE IF NOT EXISTS node_plan_job (
    node_plan_id INTEGER REFERENCES node_plan(id),
    order_index INTEGER,
    start_at BIGINT,
    duration INTEGER
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_node_plan_org ON node_plan(org_name);
CREATE INDEX IF NOT EXISTS idx_node_plan_node_id ON node_plan(node_id);
CREATE INDEX IF NOT EXISTS idx_node_plan_gpu_class ON node_plan(gpu_class_id);
CREATE INDEX IF NOT EXISTS idx_node_plan_stop_at ON node_plan(stop_at);
CREATE INDEX IF NOT EXISTS idx_node_plan_job_plan_id ON node_plan_job(node_plan_id);
