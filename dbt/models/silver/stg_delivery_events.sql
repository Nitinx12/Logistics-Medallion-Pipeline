{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select
        _id,
        event_id,
        trip_id,
        load_id,
        facility_id,
        event_type,
        scheduled_datetime,
        actual_datetime,
        on_time_flag,
        detention_minutes,
        location_city,
        location_state,
        event_ts,
        updated_at
    from {{ source('bronze', 'delivery_events') }}

    {% if is_incremental() %}
    where updated_at::timestamp > (
        select coalesce(max(updated_at), '1900-01-01'::timestamp) from {{ this }}
    )
    {% endif %}

),

cleaned as (

    select
        _id::string                            as mongo_id,
        event_id::string                       as event_id,
        trip_id::string                        as trip_id,
        load_id::string                        as load_id,
        facility_id::string                    as facility_id,
        lower(trim(event_type))                as event_type,
        scheduled_datetime::timestamp          as scheduled_datetime,
        actual_datetime::timestamp             as actual_datetime,
        -- assumes 'true'/'false' text; recheck source values (see note above)
        on_time_flag::boolean                  as on_time_flag,
        detention_minutes::bigint              as detention_minutes,
        trim(location_city)                    as location_city,
        upper(trim(location_state))            as location_state,
        event_ts::timestamp                    as event_ts,
        updated_at::timestamp                  as updated_at
    from source

),

-- dedupe within the incoming batch only; row_num is dropped before the
-- final select so it never becomes a persisted key
deduplicated as (

    select
        *,
        row_number() over (
            partition by event_id
            order by updated_at desc
        ) as row_num
    from cleaned

)

select
    mongo_id,
    event_id,
    trip_id,
    load_id,
    facility_id,
    event_type,
    scheduled_datetime,
    actual_datetime,
    on_time_flag,
    detention_minutes,
    location_city,
    location_state,
    event_ts,
    updated_at
from deduplicated
where row_num = 1
