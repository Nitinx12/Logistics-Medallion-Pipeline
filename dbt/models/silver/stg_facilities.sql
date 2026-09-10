{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='facility_id',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select
        facility_id,
        facility_name,
        facility_type,
        city,
        state,
        latitude,
        longitude,
        dock_doors,
        operating_hours,
        updated_at
    from {{ source('bronze', 'facilities') }}

    {% if is_incremental() %}
    where updated_at::timestamp > (
        select coalesce(max(updated_at), '1900-01-01'::timestamp) from {{ this }}
    )
    {% endif %}

),

cleaned as (

    select
        facility_id::string                as facility_id,
        trim(facility_name)                as facility_name,
        lower(trim(facility_type))         as facility_type,
        trim(city)                         as city,
        upper(trim(state))                 as state,
        latitude::double                   as latitude,
        longitude::double                  as longitude,
        dock_doors::bigint                 as dock_doors,
        trim(operating_hours)              as operating_hours,
        updated_at::timestamp              as updated_at
    from source

),

-- dedupe within the incoming batch only; row_num is dropped before the
-- final select so it never becomes a persisted key
deduplicated as (

    select
        *,
        row_number() over (
            partition by facility_id
            order by updated_at desc
        ) as row_num
    from cleaned

)

select
    facility_id,
    facility_name,
    facility_type,
    city,
    state,
    latitude,
    longitude,
    dock_doors,
    operating_hours,
    updated_at
from deduplicated
where row_num = 1