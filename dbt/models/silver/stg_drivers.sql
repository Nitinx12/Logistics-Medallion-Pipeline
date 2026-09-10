{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='driver_id',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select
        driver_id,
        first_name,
        last_name,
        hire_date,
        termination_date,
        license_number,
        license_state,
        date_of_birth,
        home_terminal,
        employment_status,
        cdl_class,
        years_experience,
        updated_at
    from {{ source('bronze', 'drivers') }}

    {% if is_incremental() %}
    where updated_at::timestamp > (
        select coalesce(max(updated_at), '1900-01-01'::timestamp) from {{ this }}
    )
    {% endif %}

),

cleaned as (

    select
        driver_id::string                          as driver_id,
        trim(first_name)                            as first_name,
        trim(last_name)                             as last_name,
        CAST(hire_date AS date)                     as hire_date,
        CAST(nullif(termination_date, '') AS date)  as termination_date,
        trim(license_number)                        as license_number,
        upper(trim(license_state))                  as license_state,
        CAST(date_of_birth AS date)                 as date_of_birth,
        trim(home_terminal)                         as home_terminal,
        lower(trim(employment_status))              as employment_status,
        upper(trim(cdl_class))                       as cdl_class,
        years_experience::bigint                    as years_experience,
        updated_at::timestamp                       as updated_at
    from source

),

-- dedupe within the incoming batch only; row_num is dropped before the
-- final select so it never becomes a persisted key
deduplicated as (

    select
        *,
        row_number() over (
            partition by driver_id
            order by updated_at desc
        ) as row_num
    from cleaned

)

select
    driver_id,
    first_name,
    last_name,
    hire_date,
    termination_date,
    license_number,
    license_state,
    date_of_birth,
    home_terminal,
    employment_status,
    cdl_class,
    years_experience,
    updated_at
from deduplicated
where row_num = 1
