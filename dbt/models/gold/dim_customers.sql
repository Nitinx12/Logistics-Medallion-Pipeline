{{
    config(
        materialized='table'
    )
}}

WITH snapshot AS (
    SELECT
        customer_id,
        customer_name,
        customer_type,
        credit_terms_days,
        primary_freight_type,
        account_status,
        contract_start_date,
        annual_revenue_potential,
        updated_at,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('customers_snapshot') }}
)

SELECT
    -- surrogate key for SCD2 dimension
    SHA2(
        CONCAT_WS('|', customer_id, CAST(dbt_valid_from AS STRING)),
        256
    ) AS customer_sk,
    customer_id,
    customer_name,
    customer_type,
    credit_terms_days,
    primary_freight_type,
    account_status,
    contract_start_date,
    annual_revenue_potential,
    updated_at,
    dbt_valid_from AS effective_from,
    dbt_valid_to AS effective_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM snapshot
