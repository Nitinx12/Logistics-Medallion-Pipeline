{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='maintenance_id',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select
        -- _id is excluded by the bronze extractor ({"_id": 0}); select only
        -- actual columns that land in the parquet file
        maintenance_id,
        truck_id,
        maintenance_date,
        maintenance_type,
        service_description,
        odometer_reading,
        downtime_hours,
        labor_hours,
        labor_cost,
        parts_cost,
        total_cost,
        facility_location,
        event_ts,
        updated_at
    from {{ source('bronze', 'maintenance_records') }}

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
        maintenance_id::string                 as maintenance_id,
        truck_id::string                       as truck_id,
        CAST(maintenance_date AS date)         as maintenance_date,
        lower(trim(maintenance_type))          as maintenance_type,
        trim(service_description)              as service_description,
        odometer_reading::bigint               as odometer_reading,
        downtime_hours::double                 as downtime_hours,
        labor_hours::double                    as labor_hours,
        labor_cost::double                     as labor_cost,
        parts_cost::double                     as parts_cost,
        total_cost::double                     as total_cost,
        trim(facility_location)                as facility_location,
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
            partition by maintenance_id
            order by updated_at desc
        ) as row_num
    from cleaned

)

select
    mongo_id,
    maintenance_id,
    truck_id,
    maintenance_date,
    maintenance_type,
    service_description,
    odometer_reading,
    downtime_hours,
    labor_hours,
    labor_cost,
    parts_cost,
    total_cost,
    facility_location,
    event_ts,
    updated_at
from deduplicated
where row_num = 1
