{{ config(materialized='table') }}

/*
SCD Type 2 for vehicles (trucks), sourced from the vehicles_snapshot (dbt
native snapshot, timestamp strategy on updated_at). Mirrors dim_driver.
*/

WITH ranked AS (
  SELECT
    TRIM(truck_id) AS truck_id,
    LOWER(TRIM(make)) AS make,
    TRIM(status) AS status,
    model_year,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to
  FROM {{ ref('vehicles_snapshot') }}
)
SELECT
  md5(concat(truck_id, CAST(valid_from AS STRING))) AS vehicle_sk,
  truck_id,
  make,
  model_year,
  status,
  valid_from,
  COALESCE(valid_to, '9999-12-31'::TIMESTAMP) AS valid_to,
  valid_to IS NULL AS is_current
FROM ranked
