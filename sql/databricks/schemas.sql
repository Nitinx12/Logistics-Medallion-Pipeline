-- FreightLake — Databricks lakehouse schemas
-- Run this in Databricks SQL Warehouse or a notebook attached to a cluster.
-- Requires a token with `sql` scope or OAuth. If you see
-- `Provided access token does not have required scopes: sql`,
-- regenerate the token with SQL scope or use Databricks OAuth.
-- See .env.example and docs/PROJECT_PLAN.md section 9.

-- ---------------------------------------------------------------------------
-- Unity Catalog path (preferred). On Community Edition or workspace without
-- Unity Catalog, comment out the CATALOG line and use Hive metastore:
--   CREATE SCHEMA IF NOT EXISTS bronze;
-- ---------------------------------------------------------------------------

CREATE CATALOG IF NOT EXISTS freightlake
  COMMENT 'FreightLake logistics medallion lakehouse';

CREATE SCHEMA IF NOT EXISTS freightlake.bronze
  COMMENT 'Raw landing, watermark incremental, MERGE INTO upserts, partitioned by ingestion date. See spark_jobs/bronze/';
CREATE SCHEMA IF NOT EXISTS freightlake.silver
  COMMENT 'Cleaned, deduped, conformed tables via dbt incremental merge. Snapshots (drivers_snapshot/vehicles_snapshot) also land here. See dbt/models/silver/ and dbt/snapshots/';
CREATE SCHEMA IF NOT EXISTS freightlake.gold
  COMMENT 'Star schema: SCD2 dims dim_driver/dim_vehicle + facts fct_orders/fct_shipments/fct_deliveries. See dbt/models/gold/';

-- Audit / watermark table shared by bronze jobs
CREATE TABLE IF NOT EXISTS freightlake.bronze.etl_watermark (
  source_system STRING NOT NULL,
  source_table  STRING NOT NULL,
  watermark_ts  TIMESTAMP NOT NULL,
  updated_at  TIMESTAMP NOT NULL
    COMMENT 'Last successful high watermark per source. Used for incremental extraction.',
  CONSTRAINT pk_watermark PRIMARY KEY (source_system, source_table)
)
COMMENT 'Bronze watermark table for incremental loads';

-- Verify
SHOW SCHEMAS IN freightlake;
SHOW TABLES IN freightlake.bronze;