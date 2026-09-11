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

UNION ALL

-- Unknown member: keeps fact_loads.customer_sk NOT NULL even when a load's
-- customer_id doesn't match any current row in the customer snapshot.
SELECT
    SHA2('UNKNOWN', 256) AS customer_sk,
    'UNKNOWN' AS customer_id,
    'Unknown' AS customer_name,
    'Contract' AS customer_type,
    CAST(NULL AS INT) AS credit_terms_days,
    'General' AS primary_freight_type,
    'Active' AS account_status,
    CAST(NULL AS DATE) AS contract_start_date,
    CAST(NULL AS DOUBLE) AS annual_revenue_potential,
    CAST(NULL AS TIMESTAMP) AS updated_at,
    TIMESTAMP '1900-01-01 00:00:00' AS effective_from,
    CAST(NULL AS TIMESTAMP) AS effective_to,
    TRUE AS is_current