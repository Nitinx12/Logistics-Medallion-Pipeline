{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='truck_id',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select
        truck_id,
        unit_number,
        make,
        model_year,
        vin,
        acquisition_date,
        acquisition_mileage,
        fuel_type,
        tank_capacity_gallons,
        status,
        home_terminal,
        updated_at
    from {{ source('bronze', 'trucks') }}

    {% if is_incremental() %}
    where updated_at::timestamp > (
        select coalesce(max(updated_at), '1900-01-01'::timestamp) from {{ this }}
    )
    {% endif %}

),

cleaned as (

    select
        truck_id::string                   as truck_id,
        unit_number::bigint                as unit_number,
        lower(trim(make))                  as make,
        model_year::bigint                 as model_year,
        upper(trim(vin))                   as vin,
        CAST(acquisition_date AS date)     as acquisition_date,
        acquisition_mileage::bigint        as acquisition_mileage,
        lower(trim(fuel_type))             as fuel_type,
        tank_capacity_gallons::bigint      as tank_capacity_gallons,
        lower(trim(status))                as status,
        trim(home_terminal)                as home_terminal,
        updated_at::timestamp              as updated_at
    from source

),

-- dedupe within the incoming batch only; row_num is dropped before the
-- final select so it never becomes a persisted key
deduplicated as (

    select
        *,
        row_number() over (
            partition by truck_id
            order by updated_at desc
        ) as row_num
    from cleaned

)

select
    truck_id,
    unit_number,
    make,
    model_year,
    vin,
    acquisition_date,
    acquisition_mileage,
    fuel_type,
    tank_capacity_gallons,
    status,
    home_terminal,
    updated_at
from deduplicated
where row_num = 1
