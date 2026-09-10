{{ config(materialized='table') }}

/*
Conformed customer dimension, Type 1.
Sourced from silver stg_customers. Facility attributes rarely change so Type 1
is sufficient; customer contract terms could be promoted to SCD2 later via
customers_snapshot if needed.
*/

select
    customer_id,
    trim(customer_name) as customer_name,
    customer_type,
    account_status,
    contract_start_date,
    annual_revenue_potential,
    credit_terms_days,
    primary_freight_type
from {{ ref('stg_customers') }}
