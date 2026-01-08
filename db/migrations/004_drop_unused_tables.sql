-- Drop unused tables

-- Legacy stats tables (no longer queried)
DROP TABLE IF EXISTS hourly_gpu_stats;
DROP TABLE IF EXISTS hourly_distinct_counts;
DROP TABLE IF EXISTS daily_distinct_counts;

-- Never used
DROP TABLE IF EXISTS country_snapshots;
DROP TABLE IF EXISTS node_plan_job;
DROP TABLE IF EXISTS json_import_file;
