-- ===============================================
-- clear-database.sql
-- Drop all tables to reset the database
-- ===============================================

-- Drop tables in reverse dependency order to avoid constraint issues
DROP TABLE IF EXISTS hourly_gpu_stats;
DROP TABLE IF EXISTS hourly_distinct_counts;
DROP TABLE IF EXISTS daily_distinct_counts;
-- DROP TABLE IF EXISTS gpu_classes;
-- DROP TABLE IF EXISTS city_snapshots;
-- DROP TABLE IF EXISTS country_snapshots;
