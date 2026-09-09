{{ config(materialized='incremental', unique_key='truck_id') }}

SELECT
  TRIM(truck_id) AS truck_id,
  TRIM(unit_number) AS unit_number,
  TRIM(make) AS make,
  model_year::INT AS model_year,
  TRIM(status) AS status,
  TRIM(home_terminal) AS home_terminal,
  updated_at::TIMESTAMP AS updated_at,
  _loaded_at
FROM {{ source('bronze', 'trucks') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
