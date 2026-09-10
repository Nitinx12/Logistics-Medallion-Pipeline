{{ config(materialized='table') }}

/*
Shipment fact from stg_trips.
Grain is trip_id. When joining to SCD2 dims dim_driver or dim_vehicle,
match the version where dispatch_date between valid_from and valid_to.
*/

select
  trip_id as shipment_id,
  load_id as order_id,
  driver_id,
  truck_id,
  trailer_id,
  dispatch_date as ship_date,
{% if target.type == 'duckdb' %}
  CAST(strftime(dispatch_date, '%Y%m%d') AS INT) AS date_id,
{% else %}
  CAST(date_format(dispatch_date, 'yyyyMMdd') AS INT) AS date_id,
{% endif %}
  actual_distance_miles as distance_miles,
  actual_duration_hours as duration_hours,
  fuel_gallons_used,
  average_mpg,
  idle_time_hours,
  trip_status,
  updated_at
from {{ ref('stg_trips') }}
