{{ config(materialized='table') }}

SELECT
  TRIM(trip_id) AS shipment_id,
  TRIM(load_id) AS order_id,
  TRIM(driver_id) AS driver_id,
  TRIM(truck_id) AS vehicle_id,
  dispatch_date::DATE AS ship_date,
  actual_distance_miles::DOUBLE AS distance_miles,
  actual_duration_hours::DOUBLE AS duration_hours
FROM {{ ref('stg_trips') }}
