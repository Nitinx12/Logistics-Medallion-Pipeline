{{ config(materialized='table') }}

/*
SCD Type 2 for drivers, sourced from the drivers_snapshot (dbt native
snapshot, timestamp strategy on updated_at). The snapshot already tracks
dbt_valid_from / dbt_valid_to per version, so this model just reshapes
that into a business-friendly dimension with a stable surrogate key.
*/

WITH ranked AS (
  SELECT
    TRIM(driver_id) AS driver_id,
    TRIM(first_name) || ' ' || TRIM(last_name) AS driver_name,
    TRIM(employment_status) AS employment_status,
    TRIM(home_terminal) AS region,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to
  FROM {{ ref('drivers_snapshot') }}
)
SELECT
  md5(concat(driver_id, CAST(valid_from AS STRING))) AS driver_sk,
  driver_id,
  driver_name,
  employment_status,
  region,
  valid_from,
  COALESCE(valid_to, '9999-12-31'::TIMESTAMP) AS valid_to,
  valid_to IS NULL AS is_current
FROM ranked