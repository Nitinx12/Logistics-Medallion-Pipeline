{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='incident_id',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select
        -- _id is excluded by the bronze extractor ({"_id": 0}); select only
        -- actual columns that land in the parquet file
        incident_id,
        trip_id,
        truck_id,
        driver_id,
        incident_date,
        incident_type,
        description,
        at_fault_flag,
        preventable_flag,
        injury_flag,
        location_city,
        location_state,
        cargo_damage_cost,
        vehicle_damage_cost,
        claim_amount,
        event_ts,
        updated_at
    from {{ source('bronze', 'safety_incidents') }}

    {% if is_incremental() %}
    where updated_at::timestamp > (
        select coalesce(max(updated_at), '1900-01-01'::timestamp) from {{ this }}
    )
    {% endif %}

),

cleaned as (

    select
        -- mongo_id kept as null so downstream consumers do not break when
        -- the column is referenced; the extractor excludes _id intentionally
        null::string                           as mongo_id,
        incident_id::string                    as incident_id,
        trip_id::string                        as trip_id,
        truck_id::string                       as truck_id,
        driver_id::string                      as driver_id,
        to_date(incident_date)                 as incident_date,
        lower(trim(incident_type))             as incident_type,
        trim(description)                      as description,
        -- assumes 'true'/'false' text; recheck source values (see note above)
        at_fault_flag::boolean                 as at_fault_flag,
        preventable_flag::boolean              as preventable_flag,
        injury_flag::boolean                   as injury_flag,
        trim(location_city)                    as location_city,
        upper(trim(location_state))            as location_state,
        cargo_damage_cost::double              as cargo_damage_cost,
        vehicle_damage_cost::double            as vehicle_damage_cost,
        claim_amount::double                   as claim_amount,
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
            partition by incident_id
            order by updated_at desc
        ) as row_num
    from cleaned

)

select
    mongo_id,
    incident_id,
    trip_id,
    truck_id,
    driver_id,
    incident_date,
    incident_type,
    description,
    at_fault_flag,
    preventable_flag,
    injury_flag,
    location_city,
    location_state,
    cargo_damage_cost,
    vehicle_damage_cost,
    claim_amount,
    event_ts,
    updated_at
from deduplicated
where row_num = 1
