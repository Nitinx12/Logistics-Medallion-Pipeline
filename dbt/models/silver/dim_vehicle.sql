{{ config(materialized='table') }}

/*
SCD Type 2 for vehicles (trucks) — mirrors dim_driver.
See snapshots/vehicles_snapshot.sql for alternative snapshot approach.
*/

WITH ranked AS (
  SELECT
    TRIM(truck_id) AS truck_id,
    TRIM(make) AS make,
    TRIM(status) AS status,
    updated_at::TIMESTAMP AS valid_from,
    LEAD(updated_at::TIMESTAMP) OVER (PARTITION BY TRIM(truck_id) ORDER BY updated_at) AS valid_to
  FROM {{ source('bronze', 'trucks') }}
)
SELECT
  md5(concat(truck_id, CAST(valid_from AS STRING))) AS vehicle_sk,
  truck_id,
  make,
  status,
  valid_from,
  COALESCE(valid_to, '9999-12-31'::TIMESTAMP) AS valid_to,
  valid_to IS NULL AS is_current
FROM ranked
