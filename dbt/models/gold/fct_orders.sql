{{ config(materialized='table') }}

SELECT
  l.load_id AS order_id,
  l.customer_id,
  l.route_id,
  l.load_date,
  l.revenue,
  l.fuel_surcharge,
  l.accessorial_charges,
  l.weight_lbs,
  l.pieces
FROM {{ ref('stg_loads') }} l
