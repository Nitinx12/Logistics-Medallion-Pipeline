{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='route_id',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select
        route_id,
        origin_city,
        origin_state,
        destination_city,
        destination_state,
        typical_distance_miles,
        base_rate_per_mile,
        fuel_surcharge_rate,
        typical_transit_days,
        updated_at
    from {{ source('bronze', 'routes') }}

    {% if is_incremental() %}
    where updated_at::timestamp > (
        select coalesce(max(updated_at), '1900-01-01'::timestamp) from {{ this }}
    )
    {% endif %}

),

cleaned as (

    select
        route_id::string                   as route_id,
        trim(origin_city)                  as origin_city,
        upper(trim(origin_state))          as origin_state,
        trim(destination_city)             as destination_city,
        upper(trim(destination_state))     as destination_state,
        typical_distance_miles::bigint     as typical_distance_miles,
        base_rate_per_mile::double         as base_rate_per_mile,
        fuel_surcharge_rate::double        as fuel_surcharge_rate,
        typical_transit_days::bigint       as typical_transit_days,
        updated_at::timestamp              as updated_at
    from source

),

-- dedupe within the incoming batch only; row_num is dropped before the
-- final select so it never becomes a persisted key
deduplicated as (

    select
        *,
        row_number() over (
            partition by route_id
            order by updated_at desc
        ) as row_num
    from cleaned

)

select
    route_id,
    origin_city,
    origin_state,
    destination_city,
    destination_state,
    typical_distance_miles,
    base_rate_per_mile,
    fuel_surcharge_rate,
    typical_transit_days,
    updated_at
from deduplicated
where row_num = 1