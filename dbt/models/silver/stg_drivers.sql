{{ config(materialized='incremental', unique_key='driver_id') }}

SELECT
  TRIM(driver_id) AS driver_id,
  TRIM(first_name) || ' ' || TRIM(last_name) AS driver_name,
  TRIM(employment_status) AS employment_status,
  TRIM(home_terminal) AS region,
  hire_date::DATE AS hire_date,
  updated_at::TIMESTAMP AS updated_at,
  _loaded_at
FROM {{ source('bronze', 'drivers') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
