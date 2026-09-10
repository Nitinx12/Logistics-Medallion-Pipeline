{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='fuel_purchase_id',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select
        fuel_purchase_id,
        trip_id,
        truck_id,
        driver_id,
        purchase_date,
        location_city,
        location_state,
        gallons,
        price_per_gallon,
        total_cost,
        fuel_card_number,
        updated_at
    from {{ source('bronze', 'fuel_purchases') }}

    {% if is_incremental() %}
    where updated_at::timestamp > (
        select coalesce(max(updated_at), '1900-01-01'::timestamp) from {{ this }}
    )
    {% endif %}

),

cleaned as (

    select
        fuel_purchase_id::string           as fuel_purchase_id,
        trip_id::string                    as trip_id,
        truck_id::string                   as truck_id,
        driver_id::string                  as driver_id,
        to_date(purchase_date)             as purchase_date,
        trim(location_city)                as location_city,
        upper(trim(location_state))        as location_state,
        gallons::double                    as gallons,
        price_per_gallon::double           as price_per_gallon,
        total_cost::double                 as total_cost,
        trim(fuel_card_number)             as fuel_card_number,
        updated_at::timestamp              as updated_at
    from source

),

-- dedupe within the incoming batch only; row_num is dropped before the
-- final select so it never becomes a persisted key
deduplicated as (

    select
        *,
        row_number() over (
            partition by fuel_purchase_id
            order by updated_at desc
        ) as row_num
    from cleaned

)

select
    fuel_purchase_id,
    trip_id,
    truck_id,
    driver_id,
    purchase_date,
    location_city,
    location_state,
    gallons,
    price_per_gallon,
    total_cost,
    fuel_card_number,
    updated_at
from deduplicated
where row_num = 1
