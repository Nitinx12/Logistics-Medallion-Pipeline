{% snapshot customers_snapshot %}

{{
    config(
        target_schema="snapshots",
        unique_key="customer_id",
        strategy="timestamp",
        updated_at="updated_at",
        invalidate_hard_deletes=True,
    )
}}

SELECT
    customer_id,
    customer_name,
    customer_type,
    credit_terms_days,
    primary_freight_type,
    account_status,
    contract_start_date,
    annual_revenue_potential,
    updated_at
FROM {{ source('bronze', 'customers') }}

{% endsnapshot %}
