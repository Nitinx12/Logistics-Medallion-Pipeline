-- FreightLake — watermark helpers (Databricks SQL)
-- Run after schemas.sql. Uses MERGE INTO for Databricks Delta idempotent seeding.
-- Postgres ON CONFLICT DO NOTHING does not exist on Databricks.

-- Get current watermark for a source
-- SELECT watermark_ts FROM freightlake.bronze.etl_watermark
-- WHERE source_system = 'postgres_oltp' AND source_table = 'loads';

-- Upsert watermark after successful bronze load
-- MERGE INTO freightlake.bronze.etl_watermark AS tgt
-- USING (SELECT 'postgres_oltp' AS source_system, 'loads' AS source_table,
--               max_updated_at::TIMESTAMP AS watermark_ts, now() AS updated_at
--        FROM temp_watermark) AS src
-- ON tgt.source_system = src.source_system AND tgt.source_table = src.source_table
-- WHEN MATCHED THEN UPDATE SET watermark_ts = src.watermark_ts, updated_at = src.updated_at
-- WHEN NOT MATCHED THEN INSERT (source_system, source_table, watermark_ts, updated_at)
--   VALUES (src.source_system, src.source_table, src.watermark_ts, src.updated_at);

-- Seed initial watermarks at epoch so first load pulls everything
-- Idempotent MERGE pattern, safe to rerun
MERGE INTO freightlake.bronze.etl_watermark AS tgt
USING (
  SELECT * FROM VALUES
    ('postgres_oltp', 'customers', CAST('1970-01-01 00:00:00' AS TIMESTAMP), current_timestamp()),
    ('postgres_oltp', 'drivers', CAST('1970-01-01 00:00:00' AS TIMESTAMP), current_timestamp()),
    ('postgres_oltp', 'trucks', CAST('1970-01-01 00:00:00' AS TIMESTAMP), current_timestamp()),
    ('postgres_oltp', 'trailers', CAST('1970-01-01 00:00:00' AS TIMESTAMP), current_timestamp()),
    ('postgres_oltp', 'facilities', CAST('1970-01-01 00:00:00' AS TIMESTAMP), current_timestamp()),
    ('postgres_oltp', 'routes', CAST('1970-01-01 00:00:00' AS TIMESTAMP), current_timestamp()),
    ('postgres_oltp', 'loads', CAST('1970-01-01 00:00:00' AS TIMESTAMP), current_timestamp()),
    ('postgres_oltp', 'trips', CAST('1970-01-01 00:00:00' AS TIMESTAMP), current_timestamp()),
    ('postgres_oltp', 'fuel_purchases', CAST('1970-01-01 00:00:00' AS TIMESTAMP), current_timestamp()),
    ('mongo', 'delivery_events', CAST('1970-01-01 00:00:00' AS TIMESTAMP), current_timestamp()),
    ('mongo', 'safety_incidents', CAST('1970-01-01 00:00:00' AS TIMESTAMP), current_timestamp()),
    ('mongo', 'maintenance_records', CAST('1970-01-01 00:00:00' AS TIMESTAMP), current_timestamp())
  AS s(source_system, source_table, watermark_ts, updated_at)
) AS src
ON tgt.source_system = src.source_system AND tgt.source_table = src.source_table
WHEN NOT MATCHED THEN INSERT (source_system, source_table, watermark_ts, updated_at)
  VALUES (src.source_system, src.source_table, src.watermark_ts, src.updated_at);
