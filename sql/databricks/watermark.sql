-- FreightLake — watermark helpers (Databricks SQL)

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
INSERT INTO freightlake.bronze.etl_watermark (source_system, source_table, watermark_ts, updated_at)
VALUES
  ('postgres_oltp', 'customers', '1970-01-01 00:00:00', now()),
  ('postgres_oltp', 'drivers', '1970-01-01 00:00:00', now()),
  ('postgres_oltp', 'trucks', '1970-01-01 00:00:00', now()),
  ('postgres_oltp', 'trailers', '1970-01-01 00:00:00', now()),
  ('postgres_oltp', 'facilities', '1970-01-01 00:00:00', now()),
  ('postgres_oltp', 'routes', '1970-01-01 00:00:00', now()),
  ('postgres_oltp', 'loads', '1970-01-01 00:00:00', now()),
  ('postgres_oltp', 'trips', '1970-01-01 00:00:00', now()),
  ('postgres_oltp', 'fuel_purchases', '1970-01-01 00:00:00', now()),
  ('mongo', 'delivery_events', '1970-01-01 00:00:00', now()),
  ('mongo', 'safety_incidents', '1970-01-01 00:00:00', now()),
  ('mongo', 'maintenance_records', '1970-01-01 00:00:00', now())
ON CONFLICT DO NOTHING;
