{{ config(materialized='incremental', unique_key='customer_id') }}

SELECT
  TRIM(customer_id) AS customer_id,
  TRIM(customer_name) AS customer_name,
  TRIM(customer_type) AS customer_type,
  credit_terms_days::INT AS credit_terms_days,
  TRIM(primary_freight_type) AS primary_freight_type,
  TRIM(account_status) AS account_status,
  contract_start_date::DATE AS contract_start_date,
  annual_revenue_potential::DOUBLE AS annual_revenue_potential,
  updated_at::TIMESTAMP AS updated_at,
  _loaded_at
FROM {{ source('bronze', 'customers') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
