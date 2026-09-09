{{ config(materialized='table') }}

/*
SCD Type 2 for drivers via dbt snapshot or incremental merge.
This model demonstrates valid_from valid_to is_current pattern.
For portfolio, see also snapshots/drivers_snapshot.sql.
*/

WITH ranked AS (
  SELECT
    TRIM(driver_id) AS driver_id,
    TRIM(first_name) || ' ' || TRIM(last_name) AS driver_name,
    TRIM(employment_status) AS employment_status,
    TRIM(home_terminal) AS region,
    updated_at::TIMESTAMP AS valid_from,
    LEAD(updated_at::TIMESTAMP) OVER (PARTITION BY TRIM(driver_id) ORDER BY updated_at) AS valid_to
  FROM {{ source('bronze', 'drivers') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['driver_id', 'valid_from']) }} AS driver_sk,
  driver_id,
  driver_name,
  employment_status,
  region,
  valid_from,
  COALESCE(valid_to, '9999-12-31'::TIMESTAMP) AS valid_to,
  valid_to IS NULL AS is_current
FROM ranked
