{{ config(materialized='table') }}

SELECT
  TRIM(facility_id) AS warehouse_id,
  TRIM(facility_name) AS warehouse_name,
  TRIM(facility_type) AS warehouse_type,
  TRIM(city) AS city,
  TRIM(state) AS state
FROM {{ source('bronze', 'facilities') }}
