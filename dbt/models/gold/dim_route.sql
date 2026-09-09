{{ config(materialized='table') }}

SELECT
  TRIM(route_id) AS route_id,
  TRIM(origin_city) AS origin_city,
  TRIM(origin_state) AS origin_state,
  TRIM(destination_city) AS destination_city,
  TRIM(destination_state) AS destination_state,
  typical_distance_miles::DOUBLE AS distance_miles,
  base_rate_per_mile::DOUBLE AS rate_per_mile
FROM {{ source('bronze', 'routes') }}
