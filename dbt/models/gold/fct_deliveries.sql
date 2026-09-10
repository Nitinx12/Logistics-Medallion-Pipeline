{{ config(materialized='table') }}

SELECT
  TRIM(event_id) AS delivery_id,
  TRIM(load_id) AS order_id,
  TRIM(trip_id) AS shipment_id,
  event_ts::TIMESTAMP AS delivery_ts,
  TRIM(event_type) AS event_type,
  detention_minutes::INT AS detention_minutes,
  on_time_flag::BOOLEAN AS is_on_time
FROM {{ source('bronze', 'delivery_events') }}
