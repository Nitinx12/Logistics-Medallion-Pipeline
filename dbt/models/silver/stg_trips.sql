{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trip_id',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select
        trip_id,
        load_id,
        driver_id,
        truck_id,
        trailer_id,
        dispatch_date,
        actual_distance_miles,
        actual_duration_hours,
        fuel_gallons_used,
        average_mpg,
        idle_time_hours,
        trip_status,
        updated_at
    from {{ source('bronze', 'trips') }}

    {% if is_incremental() %}
    where updated_at::timestamp > (
        select coalesce(max(updated_at), '1900-01-01'::timestamp) from {{ this }}
    )
    {% endif %}

),

cleaned as (

    select
        trip_id::string                     as trip_id,
        load_id::string                     as load_id,
        driver_id::string                   as driver_id,
        truck_id::string                    as truck_id,
        trailer_id::string                  as trailer_id,
        CAST(dispatch_date AS date)         as dispatch_date,
        actual_distance_miles::bigint       as actual_distance_miles,
        actual_duration_hours::double       as actual_duration_hours,
        fuel_gallons_used::double           as fuel_gallons_used,
        average_mpg::double                 as average_mpg,
        idle_time_hours::double             as idle_time_hours,
        lower(trim(trip_status))            as trip_status,
        updated_at::timestamp               as updated_at
    from source

),

-- dedupe within the incoming batch only; row_num is dropped before the
-- final select so it never becomes a persisted key
deduplicated as (

    select
        *,
        row_number() over (
            partition by trip_id
            order by updated_at desc
        ) as row_num
    from cleaned

)

select
    trip_id,
    load_id,
    driver_id,
    truck_id,
    trailer_id,
    dispatch_date,
    actual_distance_miles,
    actual_duration_hours,
    fuel_gallons_used,
    average_mpg,
    idle_time_hours,
    trip_status,
    updated_at
from deduplicated
where row_num = 1
