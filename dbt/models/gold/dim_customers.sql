{{ config(materialized='table') }}

SELECT
  customer_id,
  customer_name,
  customer_type,
  account_status,
  contract_start_date,
  annual_revenue_potential
FROM {{ ref('stg_customers') }}
