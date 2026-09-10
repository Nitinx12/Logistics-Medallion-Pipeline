{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        file_format='delta',
        on_schema_change='sync_all_columns',
        post_hook="ALTER TABLE {{ this }} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')"
    )
}}

with source as (

    select
        customer_id,
        customer_name,
        customer_type,
        credit_terms_days,
        primary_freight_type,
        account_status,
        contract_start_date,
        annual_revenue_potential,
        updated_at
    from {{ source('bronze', 'customers') }}

    {% if is_incremental() %}
    where updated_at::timestamp > (
        select coalesce(max(updated_at), '1900-01-01'::timestamp) from {{ this }}
    )
    {% endif %}

),

cleaned as (

    select
        customer_id::string                as customer_id,
        trim(customer_name)                as customer_name,
        lower(trim(customer_type))         as customer_type,
        credit_terms_days::bigint          as credit_terms_days,
        lower(trim(primary_freight_type))  as primary_freight_type,
        lower(trim(account_status))        as account_status,
        to_date(contract_start_date)       as contract_start_date,
        annual_revenue_potential::bigint   as annual_revenue_potential,
        updated_at::timestamp              as updated_at
    from source

),

-- dedupe within the incremental batch only; row_num is never persisted,
-- so it can't drift the way a row_number()-based surrogate key would
deduplicated as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by updated_at desc
        ) as row_num
    from cleaned

)

select
    customer_id,
    customer_name,
    customer_type,
    credit_terms_days,
    primary_freight_type,
    account_status,
    contract_start_date,
    annual_revenue_potential,
    updated_at
from deduplicated
where row_num = 1
