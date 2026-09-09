{{ config(materialized='incremental', unique_key='load_id') }}

SELECT
  TRIM(load_id) AS load_id,
  TRIM(customer_id) AS customer_id,
  TRIM(route_id) AS route_id,
  load_date::DATE AS load_date,
  TRIM(load_type) AS load_type,
  weight_lbs::DOUBLE AS weight_lbs,
  pieces::INT AS pieces,
  revenue::DOUBLE AS revenue,
  updated_at::TIMESTAMP AS updated_at,
  _loaded_at
FROM {{ source('bronze', 'loads') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
