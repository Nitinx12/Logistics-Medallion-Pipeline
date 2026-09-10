{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='load_id',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select
        load_id,
        customer_id,
        route_id,
        load_date,
        load_type,
        weight_lbs,
        pieces,
        revenue,
        fuel_surcharge,
        accessorial_charges,
        load_status,
        booking_type,
        updated_at
    from {{ source('bronze', 'loads') }}

    {% if is_incremental() %}
    where updated_at::timestamp > (
        select coalesce(max(updated_at), '1900-01-01'::timestamp) from {{ this }}
    )
    {% endif %}

),

cleaned as (

    select
        load_id::string                    as load_id,
        customer_id::string                as customer_id,
        route_id::string                   as route_id,
        to_date(load_date)                 as load_date,
        lower(trim(load_type))             as load_type,
        weight_lbs::bigint                 as weight_lbs,
        pieces::bigint                     as pieces,
        revenue::double                    as revenue,
        fuel_surcharge::double             as fuel_surcharge,
        accessorial_charges::bigint        as accessorial_charges,
        lower(trim(load_status))           as load_status,
        lower(trim(booking_type))          as booking_type,
        updated_at::timestamp              as updated_at
    from source

),

-- dedupe within the incoming batch only; row_num is dropped before the
-- final select so it never becomes a persisted key
deduplicated as (

    select
        *,
        row_number() over (
            partition by load_id
            order by updated_at desc
        ) as row_num
    from cleaned

)

select
    load_id,
    customer_id,
    route_id,
    load_date,
    load_type,
    weight_lbs,
    pieces,
    revenue,
    fuel_surcharge,
    accessorial_charges,
    load_status,
    booking_type,
    updated_at
from deduplicated
where row_num = 1
