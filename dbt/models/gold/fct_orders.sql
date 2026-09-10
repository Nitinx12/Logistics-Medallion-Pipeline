{{ config(materialized='table') }}

/*
Orders fact from stg_loads.
Grain is load_id. For SCD2 joins to dim_customer or dim_route, join on
load_date between valid_from and valid_to if those dims are promoted to SCD2.
*/

select
  l.load_id as order_id,
  l.customer_id,
  l.route_id,
  l.load_date,
  CAST(date_format(load_date, 'yyyyMMdd') AS INT) AS date_id,
  l.load_type,
  l.revenue,
  l.fuel_surcharge,
  l.accessorial_charges,
  l.weight_lbs,
  l.pieces,
  l.load_status,
  l.booking_type,
  l.updated_at
from {{ ref('stg_loads') }} l
