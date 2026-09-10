{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trailer_id',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select
        trailer_id,
        trailer_number,
        trailer_type,
        length_feet,
        model_year,
        vin,
        acquisition_date,
        status,
        current_location,
        updated_at
    from {{ source('bronze', 'trailers') }}

    {% if is_incremental() %}
    where updated_at::timestamp > (
        select coalesce(max(updated_at), '1900-01-01'::timestamp) from {{ this }}
    )
    {% endif %}

),

cleaned as (

    select
        trailer_id::string                 as trailer_id,
        trailer_number::bigint              as trailer_number,
        lower(trim(trailer_type))          as trailer_type,
        length_feet::bigint                as length_feet,
        model_year::bigint                 as model_year,
        upper(trim(vin))                   as vin,
        to_date(acquisition_date)          as acquisition_date,
        lower(trim(status))                as status,
        trim(current_location)             as current_location,
        updated_at::timestamp              as updated_at
    from source

),

-- dedupe within the incoming batch only; row_num is dropped before the
-- final select so it never becomes a persisted key
deduplicated as (

    select
        *,
        row_number() over (
            partition by trailer_id
            order by updated_at desc
        ) as row_num
    from cleaned

)

select
    trailer_id,
    trailer_number,
    trailer_type,
    length_feet,
    model_year,
    vin,
    acquisition_date,
    status,
    current_location,
    updated_at
from deduplicated
where row_num = 1