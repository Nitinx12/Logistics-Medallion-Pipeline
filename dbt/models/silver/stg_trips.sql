{{ config(materialized='incremental', unique_key='trip_id') }}

SELECT
  TRIM(trip_id) AS trip_id,
  TRIM(load_id) AS load_id,
  TRIM(driver_id) AS driver_id,
  TRIM(truck_id) AS truck_id,
  TRIM(trailer_id) AS trailer_id,
  dispatch_date::DATE AS dispatch_date,
  updated_at::TIMESTAMP AS updated_at,
  _loaded_at
FROM {{ source('bronze', 'trips') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
