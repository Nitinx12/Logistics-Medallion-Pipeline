{{ config(materialized='table') }}

/*
Delivery event fact, sourced from silver stg_delivery_events.
One row per pickup or delivery event, grain is event_id.
Join to SCD2 dims on event timestamp between valid_from and valid_to.
*/

select
  event_id as delivery_id,
  load_id as order_id,
  trip_id as shipment_id,
  facility_id as warehouse_id,
  event_ts as delivery_ts,
  CAST(date_format(CAST(event_ts AS DATE), 'yyyyMMdd') AS INT) AS date_id,
  event_type,
  detention_minutes,
  on_time_flag as is_on_time,
  scheduled_datetime,
  actual_datetime,
  location_city,
  location_state
from {{ ref('stg_delivery_events') }}
