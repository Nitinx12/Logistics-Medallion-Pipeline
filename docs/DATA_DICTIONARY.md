# Data Dictionary

Column level reference for all three medallion layers. The source of
truth is `dbt/models/silver/_silver.yml` and `dbt/models/gold/_gold.yml`:
when a model changes, update the yml first and mirror the change here in
the same pull request.

## How to read the tables

- The **Tests** column lists the dbt tests attached to the column in the
  yml. A plain name means severity `error`: a failure blocks the build
  and the DAG. A `(warn)` suffix means severity `warn`: a failure is
  recorded, not fatal. The warn tests mirror known data gaps documented
  in `gx/README.md` and `docs/DATA_QUALITY.md`.
- `relationships to model.column` checks referential integrity, the
  matching `no_orphan_rows` test is the project's Databricks compatible
  equivalent on the same pair.
- A `where:` clause narrows a test to a subset of rows, for example the
  unique test on `trailers.trailer_number` only applies to active
  trailers.
- `updated_at` is the incremental watermark column on every table: each
  run reads rows where `updated_at` exceeds `MAX(updated_at)` already
  landed in the target.
- The tables list every tested column from the yml. A few models carry
  additional pass through columns with no tests attached; those are
  called out under the model.

```mermaid
flowchart LR
    SRC["Sources<br/>Postgres OLTP<br/>MongoDB"]:::src
    B["Bronze<br/>freightlake.bronze<br/>12 tables, source shape"]:::bronze
    S["Silver<br/>freightlake.silver<br/>12 models, cleaned"]:::silver
    G["Gold<br/>freightlake.gold<br/>7 dimensions, 7 facts"]:::gold
    SRC --> B --> S --> G

    classDef src fill:#336791,stroke:#1d4d6b,color:#fff
    classDef bronze fill:#cd7f32,stroke:#8b5a2b,color:#fff
    classDef silver fill:#d8dee9,stroke:#7b8894,color:#222
    classDef gold fill:#ffd700,stroke:#b8860b,color:#222
```

## Bronze

Raw landing zone in the `freightlake.bronze` schema. Bronze keeps each
table in its source shape, cast for Delta compatibility, plus the
`updated_at` column every incremental extraction keys on. Bronze is an
append log: reruns add only rows newer than the watermark, never a full
reload.

| Table | Source | Watermark column |
|---|---|---|
| `customers` | Postgres OLTP, `public.customers` | `updated_at` |
| `drivers` | Postgres OLTP, `public.drivers` | `updated_at` |
| `facilities` | Postgres OLTP, `public.facilities` | `updated_at` |
| `fuel_purchases` | Postgres OLTP, `public.fuel_purchases` | `updated_at` |
| `loads` | Postgres OLTP, `public.loads` | `updated_at` |
| `routes` | Postgres OLTP, `public.routes` | `updated_at` |
| `trailers` | Postgres OLTP, `public.trailers` | `updated_at` |
| `trips` | Postgres OLTP, `public.trips` | `updated_at` |
| `trucks` | Postgres OLTP, `public.trucks` | `updated_at` |
| `delivery_events` | MongoDB, `freight_lake.delivery_events` | `updated_at` |
| `maintenance_records` | MongoDB, `freight_lake.maintenance_records` | `updated_at` |
| `safety_incidents` | MongoDB, `freight_lake.safety_incidents` | `updated_at` |

Column level tests do not exist in bronze by design: bronze lands data
as it arrived, quality enforcement starts in silver.

## Silver

Cleaned and deduplicated models in the `freightlake.silver` schema. All
models are incremental with the merge strategy on the primary key,
deduplication via `ROW_NUMBER() OVER (PARTITION BY primary key ORDER BY
updated_at DESC)`, and a three day watermark lookback. Primary keys are
always severity `error` on not null and unique.

### customers

Silver customers dimension cleansed and deduplicated from bronze
customers source. One row per customer id with latest updated at and
watermark handling.

| Column | Description | Tests |
|---|---|---|
| `customer_id` | Primary key for customers. Unique identifier from source system. | not_null, unique, no_empty_strings, no_white_spaces, matches_regex `^[A-Za-z0-9_-]+$` |
| `customer_name` | Customer display name as provided by source. | not_null, no_empty_strings, no_white_spaces |
| `customer_type` | Customer classification grouping in source. | no_empty_strings, no_white_spaces, accepted_values (Contract, Dedicated, Spot) |
| `credit_terms_days` | Payment terms in days agreed for customer. | accepted_range 0 to 120, not_null (warn) |
| `primary_freight_type` | Main freight mode used by customer. | no_empty_strings, no_white_spaces, accepted_values (Automotive, Consumer Goods, Electronics, Food/Beverage, General, Retail) |
| `account_status` | Current account lifecycle status. | not_null, no_empty_strings, no_white_spaces, accepted_values (Active, Inactive) |
| `contract_start_date` | Date when contract started with customer. | no_future_dates, not_null (warn) |
| `annual_revenue_potential` | Estimated annual revenue potential for customer in dollars. | accepted_range 0 to 100000000, not_null (warn) |
| `updated_at` | Last update timestamp from source system used for incremental watermark. | not_null, no_future_dates |

### drivers

Silver drivers dimension with employment and license attributes.

| Column | Description | Tests |
|---|---|---|
| `driver_id` | Primary key for drivers. | not_null, unique, no_empty_strings, no_white_spaces, matches_regex `^[A-Za-z0-9_-]+$` |
| `first_name` | Driver first name. | not_null, no_empty_strings, no_white_spaces, matches_regex `^[A-Za-z -]+$` |
| `last_name` | Driver last name. | not_null, no_empty_strings, no_white_spaces, matches_regex `^[A-Za-z -]+$` |
| `hire_date` | Date driver was hired. | not_null, no_future_dates |
| `termination_date` | Date driver employment ended if applicable. | no_future_dates |
| `license_number` | Commercial driver license number. | not_null, no_empty_strings, no_white_spaces, matches_regex `^[A-Za-z0-9-]+$` |
| `license_state` | State that issued the license as two letter code. | not_null, no_empty_strings, no_white_spaces, matches_regex `^[A-Z]{2}$`, accepted_values (23 states) |
| `date_of_birth` | Driver birth date. | no_future_dates, not_null (warn) |
| `home_terminal` | Home terminal location for driver. | no_empty_strings, no_white_spaces, accepted_values (25 cities) |
| `employment_status` | Current employment status. | not_null, no_empty_strings, no_white_spaces, accepted_values (Active, Terminated) |
| `cdl_class` | Commercial license class. | accepted_values (A), no_white_spaces |
| `years_experience` | Years of driving experience. | accepted_range 0 to 60, not_null (warn) |
| `updated_at` | Last update timestamp for incremental processing. | not_null, no_future_dates |

### facilities

Silver facilities dimension for warehouses and terminals.

| Column | Description | Tests |
|---|---|---|
| `facility_id` | Primary key for facilities. | not_null, unique, no_empty_strings, no_white_spaces |
| `facility_name` | Facility display name. | not_null, no_empty_strings, no_white_spaces |
| `facility_type` | Facility functional type. | not_null, no_empty_strings, no_white_spaces, accepted_values (Cross-Dock, Distribution Center, Terminal, Warehouse) |
| `city` | Facility city name. | not_null, no_empty_strings, no_white_spaces, accepted_values (21 cities) |
| `state` | Facility state code. | not_null, no_empty_strings, no_white_spaces, matches_regex `^[A-Z]{2}$`, accepted_values (20 states) |
| `latitude` | Geographic latitude coordinate. | not_null, accepted_range -90 to 90 |
| `longitude` | Geographic longitude coordinate. | not_null, accepted_range -180 to 180 |
| `dock_doors` | Number of dock doors at facility. | accepted_range 0 to 500, not_null (warn) |
| `operating_hours` | Operating hours description. | no_empty_strings (warn) |
| `updated_at` | Last update timestamp. | not_null, no_future_dates |

### routes

Silver routes master data with origin and destination and rate
attributes.

| Column | Description | Tests |
|---|---|---|
| `route_id` | Primary key for routes. | not_null, unique, no_empty_strings, no_white_spaces |
| `origin_city` | Origin city name. | not_null, no_empty_strings, no_white_spaces, accepted_values (17 cities) |
| `origin_state` | Origin state code two letters. | not_null, no_empty_strings, no_white_spaces, matches_regex `^[A-Z]{2}$`, accepted_values (17 states) |
| `destination_city` | Destination city name. | not_null, no_empty_strings, no_white_spaces, accepted_values (18 cities) |
| `destination_state` | Destination state code two letters. | not_null, no_empty_strings, no_white_spaces, matches_regex `^[A-Z]{2}$`, accepted_values (18 states) |
| `typical_distance_miles` | Typical distance for route in miles. | not_null, accepted_range 0 to 5000 |
| `base_rate_per_mile` | Base rate in dollars per mile. | not_null, accepted_range 0 to 20 |
| `fuel_surcharge_rate` | Fuel surcharge rate per mile. | accepted_range 0 to 5 |
| `typical_transit_days` | Typical transit time in days. | accepted_range 0 to 30, not_null (warn) |
| `updated_at` | Last update timestamp. | not_null, no_future_dates |

### trucks

Silver trucks dimension with acquisition and status attributes.

| Column | Description | Tests |
|---|---|---|
| `truck_id` | Primary key for trucks. | not_null, unique, no_empty_strings, no_white_spaces |
| `unit_number` | Business visible unit number for truck. | not_null, unique, no_empty_strings, no_white_spaces |
| `make` | Truck manufacturer make. | not_null, no_empty_strings, no_white_spaces, accepted_values (Freightliner, International, Kenworth, Mack, Peterbilt, Volvo) |
| `model_year` | Model year of truck. | not_null, accepted_range 1990 to 2027 |
| `vin` | Vehicle identification number seventeen characters. | not_null, no_empty_strings, no_white_spaces, matches_regex `^[A-HJ-NPR-Z0-9]{17}$` |
| `acquisition_date` | Date truck was acquired. | not_null, no_future_dates |
| `acquisition_mileage` | Odometer at acquisition. | accepted_range 0 to 2000000 |
| `fuel_type` | Fuel type used by truck. | not_null, accepted_values (Diesel), no_empty_strings, no_white_spaces |
| `tank_capacity_gallons` | Fuel tank capacity in gallons. | accepted_range 0 to 300 |
| `status` | Current operational status of truck. | not_null, no_empty_strings, no_white_spaces, accepted_values (Active, Inactive, Maintenance) |
| `home_terminal` | Home terminal for truck assignment. | no_empty_strings (warn), no_white_spaces, accepted_values (25 cities) |
| `updated_at` | Last update timestamp. | not_null, no_future_dates |

### trailers

Silver trailers dimension.

| Column | Description | Tests |
|---|---|---|
| `trailer_id` | Primary key for trailers. | not_null, unique, no_empty_strings, no_white_spaces |
| `trailer_number` | Business visible trailer number. | not_null, unique (warn, where: `status = 'Active'`), no_empty_strings, no_white_spaces |
| `trailer_type` | Trailer functional type. | not_null, accepted_values (Dry Van, Refrigerated), no_empty_strings, no_white_spaces |
| `length_feet` | Trailer length in feet. | accepted_range 20 to 100, not_null |
| `model_year` | Model year of trailer. | accepted_range 1990 to 2027 |
| `vin` | Vehicle identification number for trailer. | no_empty_strings, no_white_spaces, matches_regex `^[A-HJ-NPR-Z0-9]{17}$` (warn) |
| `acquisition_date` | Date trailer was acquired. | no_future_dates |
| `status` | Current status of trailer. | not_null, accepted_values (Active), no_empty_strings, no_white_spaces |
| `current_location` | Current location description for trailer. | no_empty_strings (warn), accepted_values (25 cities) |
| `updated_at` | Last update timestamp. | not_null, no_future_dates |

### loads

Silver loads fact style dimension with customer and route links and
financial amounts.

| Column | Description | Tests |
|---|---|---|
| `load_id` | Primary key for loads. | not_null, unique, no_empty_strings, no_white_spaces |
| `customer_id` | Customer owning the load. | not_null, no_empty_strings, no_white_spaces, relationships to customers.customer_id, no_orphan_rows |
| `route_id` | Route assigned to load. | no_empty_strings (warn), no_white_spaces, relationships to routes.route_id (warn), no_orphan_rows (warn) |
| `load_date` | Date load was created. | not_null, no_future_dates |
| `load_type` | Load categorization by size and mode. | not_null, accepted_values (Dry Van, Refrigerated), no_empty_strings, no_white_spaces |
| `weight_lbs` | Weight of load in pounds. | not_null, accepted_range 0 to 80000 |
| `pieces` | Number of pieces in load. | accepted_range 1 to 1000, not_null (warn) |
| `revenue` | Revenue for load in dollars. | not_null, accepted_range 0 to 100000 |
| `fuel_surcharge` | Fuel surcharge amount in dollars. | accepted_range 0 to 10000 |
| `accessorial_charges` | Additional accessorial charges in dollars. | accepted_range 0 to 10000 |
| `load_status` | Current lifecycle status of load. | not_null, accepted_values (Completed), no_empty_strings, no_white_spaces |
| `booking_type` | Booking channel for load. | accepted_values (Contract, Dedicated, Spot), no_empty_strings (warn), no_white_spaces |
| `updated_at` | Last update timestamp. | not_null, no_future_dates |

### trips

Silver trips fact table linking load driver truck and trailer with
performance metrics.

| Column | Description | Tests |
|---|---|---|
| `trip_id` | Primary key for trips. | not_null, unique, no_empty_strings, no_white_spaces |
| `load_id` | Load associated with trip. | not_null, no_empty_strings, no_white_spaces, relationships to loads.load_id, no_orphan_rows |
| `driver_id` | Driver assigned to trip. | not_null (warn, where: `trip_status != 'Completed'`), relationships to drivers.driver_id (warn), no_orphan_rows (warn) |
| `truck_id` | Truck assigned to trip. | not_null (warn, where: `trip_status != 'Completed'`), relationships to trucks.truck_id (warn), no_orphan_rows (warn) |
| `trailer_id` | Trailer assigned to trip. | relationships to trailers.trailer_id (warn), no_orphan_rows (warn) |
| `dispatch_date` | Date trip was dispatched. | not_null, no_future_dates |
| `actual_distance_miles` | Actual distance traveled in miles. | accepted_range 0 to 5000, not_null (warn) |
| `actual_duration_hours` | Actual duration in hours. | accepted_range 0 to 500 |
| `fuel_gallons_used` | Fuel consumed on trip in gallons. | accepted_range 0 to 1000 |
| `average_mpg` | Average miles per gallon achieved. | accepted_range 0 to 30 |
| `idle_time_hours` | Idle time during trip in hours. | accepted_range 0 to 100 |
| `trip_status` | Current status of trip. | not_null, accepted_values (Completed), no_empty_strings, no_white_spaces |
| `updated_at` | Last update timestamp. | not_null, no_future_dates |

### fuel_purchases

Silver fuel purchases events linked to trip and vehicle.

| Column | Description | Tests |
|---|---|---|
| `fuel_purchase_id` | Primary key for fuel purchases. | not_null, unique, no_empty_strings, no_white_spaces |
| `trip_id` | Trip during which fuel was purchased. | relationships to trips.trip_id (warn), no_orphan_rows (warn), no_empty_strings (warn) |
| `truck_id` | Truck that received fuel. | not_null (warn, failures stored), relationships to trucks.truck_id (warn), no_orphan_rows (warn) |
| `driver_id` | Driver who made purchase. | relationships to drivers.driver_id (warn), no_orphan_rows (warn) |
| `purchase_date` | Date of fuel purchase. | not_null, no_future_dates |
| `location_city` | City where fuel was purchased. | no_empty_strings (warn), no_white_spaces, accepted_values (25 cities) |
| `location_state` | State where fuel was purchased. | matches_regex `^[A-Z]{2}$` (warn), no_white_spaces, accepted_values (23 states) |
| `gallons` | Gallons purchased. | not_null, accepted_range 0 to 500 |
| `price_per_gallon` | Price per gallon in dollars. | not_null, accepted_range 0 to 15 |
| `total_cost` | Total purchase cost in dollars. | not_null, accepted_range 0 to 10000 |
| `fuel_card_number` | Fuel card used for purchase. | no_empty_strings (warn), no_white_spaces, matches_regex `^[A-Za-z0-9-]+$` (warn) |
| `updated_at` | Last update timestamp. | not_null, no_future_dates |

The warn severity on `truck_id` not null is the known 3880 null rows
issue documented in `gx/README.md`, with `store_failures: true` so the
failing rows are queryable in the dbt test failures table.

### delivery_events

Silver delivery events tracking pickup and delivery milestones.

| Column | Description | Tests |
|---|---|---|
| `event_id` | Primary key for delivery events. | not_null, unique, no_empty_strings, no_white_spaces |
| `load_id` | Load linked to event. | not_null, relationships to loads.load_id, no_orphan_rows |
| `trip_id` | Trip linked to event. | relationships to trips.trip_id (warn), no_orphan_rows (warn) |
| `event_type` | Type of delivery milestone. | not_null, accepted_values (Delivery, Pickup), no_empty_strings, no_white_spaces |
| `facility_id` | Facility where event occurred. | relationships to facilities.facility_id (warn), no_orphan_rows (warn) |
| `scheduled_datetime` | Scheduled date and time for event. | not_null, no_future_dates (warn) |
| `actual_datetime` | Actual date and time when event occurred. | no_future_dates |
| `detention_minutes` | Detention time in minutes. | accepted_range 0 to 1440 |
| `on_time_flag` | Flag indicating on time performance true or false. | accepted_values (True, False) |
| `location_city` | City where event occurred. | no_empty_strings (warn), no_white_spaces, accepted_values (17 cities) |
| `location_state` | State where event occurred. | matches_regex `^[A-Z]{2}$` (warn), no_white_spaces, accepted_values (19 states) |
| `updated_at` | Last update timestamp used for watermark. | not_null, no_future_dates |
| `event_ts` | Event timestamp from source for ordering. | no_future_dates |

### maintenance_records

Silver maintenance records for trucks.

| Column | Description | Tests |
|---|---|---|
| `maintenance_id` | Primary key for maintenance records. | not_null, unique, no_empty_strings, no_white_spaces |
| `truck_id` | Truck that received maintenance. | not_null, relationships to trucks.truck_id, no_orphan_rows |
| `maintenance_date` | Date maintenance was performed. | not_null, no_future_dates |
| `maintenance_type` | Category of maintenance. | not_null, accepted_values (Brake, Engine, Inspection, Preventive, Repair, Tire, Transmission), no_empty_strings, no_white_spaces |
| `odometer_reading` | Odometer reading at maintenance time. | accepted_range 0 to 3000000, not_null (warn) |
| `labor_hours` | Labor hours spent. | accepted_range 0 to 100 |
| `labor_cost` | Labor cost in dollars. | accepted_range 0 to 10000 |
| `parts_cost` | Parts cost in dollars. | accepted_range 0 to 20000 |
| `total_cost` | Total maintenance cost in dollars. | not_null, accepted_range 0 to 50000 |
| `facility_location` | Location where maintenance occurred. | no_empty_strings (warn), no_white_spaces, accepted_values (25 cities) |
| `downtime_hours` | Vehicle downtime in hours due to maintenance. | accepted_range 0 to 720 |
| `service_description` | Free text description of service. | no_empty_strings (warn) |
| `updated_at` | Last update timestamp. | not_null, no_future_dates |
| `event_ts` | Event timestamp from source. | no_future_dates |

### safety_incidents

Silver safety incidents with impact and cost attributes.

| Column | Description | Tests |
|---|---|---|
| `incident_id` | Primary key for safety incidents. | not_null, unique, no_empty_strings, no_white_spaces |
| `trip_id` | Trip during which incident occurred. | relationships to trips.trip_id (warn), no_orphan_rows (warn) |
| `truck_id` | Truck involved in incident. | relationships to trucks.truck_id (warn), no_orphan_rows (warn) |
| `driver_id` | Driver involved in incident. | relationships to drivers.driver_id (warn), no_orphan_rows (warn) |
| `incident_date` | Date incident occurred. | not_null, no_future_dates |
| `incident_type` | Category of safety incident. | not_null, accepted_values (Accident, Customer Complaint, DOT Violation, Equipment Damage, Moving Violation), no_empty_strings, no_white_spaces |
| `location_city` | City where incident occurred. | no_empty_strings (warn), no_white_spaces, accepted_values (25 cities) |
| `location_state` | State where incident occurred. | matches_regex `^[A-Z]{2}$` (warn), no_white_spaces, accepted_values (23 states) |
| `at_fault_flag` | Flag indicating driver at fault. | accepted_values (True, False) |
| `injury_flag` | Flag indicating injury occurred. | accepted_values (True, False) |
| `vehicle_damage_cost` | Vehicle damage cost in dollars. | accepted_range 0 to 1000000 |
| `cargo_damage_cost` | Cargo damage cost in dollars. | accepted_range 0 to 1000000 |
| `claim_amount` | Insurance claim amount in dollars. | accepted_range 0 to 2000000 |
| `preventable_flag` | Flag indicating incident was preventable. | accepted_values (True, False) |
| `description` | Free text incident description. | no_empty_strings (warn) |
| `updated_at` | Last update timestamp. | not_null, no_future_dates |
| `event_ts` | Event timestamp from source. | no_future_dates |

## Gold

Star schema in the `freightlake.gold` schema: seven dimensions, seven
facts. Surrogate keys (`*_sk`) are SHA2 hashes of the natural key, plus
the SCD validity timestamp on `dim_customers`. Every dimension carries
an UNKNOWN member row so fact foreign keys stay not null even when a
natural key has no match; the `is_unmatched_*` flags on facts mark those
rows explicitly. Gold yml descriptions follow the model file; tests on
surrogate keys and natural keys are always severity `error`.

### dim_customers

Gold customer dimension SCD Type 2 built from customers snapshot. One
row per version with surrogate key. The versioning comes from
`dbt/snapshots/customers_snapshot.sql` with `invalidate_hard_deletes`.

| Column | Description | Tests |
|---|---|---|
| `customer_sk` | SHA2 hash of customer_id plus the SCD validity start. | not_null, unique |
| `customer_id` | Natural key from source. | not_null, no_empty_strings, no_white_spaces |
| `customer_name` | Customer display name. | not_null, no_empty_strings |
| `customer_type` | Customer classification. | accepted_values (Contract, Dedicated, Spot) |
| `primary_freight_type` | Main freight mode. | accepted_values (Automotive, Consumer Goods, Electronics, Food/Beverage, General, Retail) |
| `account_status` | Account lifecycle status. | accepted_values (Active, Inactive) |
| `effective_from` | Start of validity for this version. | not_null, no_future_dates |
| `effective_to` | End of validity, null while current. | no_future_dates |
| `is_current` | True on the latest version of each customer. | not_null, accepted_values |

Untested pass through columns: `credit_terms_days`,
`contract_start_date`, `annual_revenue_potential`, `updated_at`. The
UNKNOWN member row uses `customer_id = 'UNKNOWN'` with
`effective_from = 1900-01-01`.

### dim_drivers

Gold driver dimension. One row per driver_id, plus the UNKNOWN member
row.

| Column | Description | Tests |
|---|---|---|
| `driver_sk` | SHA2 surrogate key. | not_null, unique |
| `driver_id` | Natural key from source. | not_null, unique, no_empty_strings, no_white_spaces |
| `driver_name` | Concatenated first and last name. | not_null, no_empty_strings |
| `employment_status` | Employment status. | accepted_values (Active, Terminated) |
| `cdl_class` | Commercial license class. | accepted_values (A) |
| `license_state` | State that issued the license. | matches_regex `^[A-Z]{2}$` |

### dim_facilities

Gold facilities dimension. One row per facility_id, plus the UNKNOWN
member row.

| Column | Description | Tests |
|---|---|---|
| `facility_sk` | SHA2 surrogate key. | not_null, unique |
| `facility_id` | Natural key from source. | not_null, unique |
| `facility_type` | Facility functional type. | accepted_values (Cross-Dock, Distribution Center, Terminal, Warehouse) |
| `state` | Facility state code. | matches_regex `^[A-Z]{2}$` |

### dim_routes

Gold routes dimension. One row per route_id, plus the UNKNOWN member
row.

| Column | Description | Tests |
|---|---|---|
| `route_sk` | SHA2 surrogate key. | not_null, unique |
| `route_id` | Natural key from source. | not_null, unique |
| `origin_state` | Origin state code. | matches_regex `^[A-Z]{2}$` |
| `destination_state` | Destination state code. | matches_regex `^[A-Z]{2}$` |

### dim_trucks

Gold trucks dimension. One row per truck_id, plus the UNKNOWN member
row.

| Column | Description | Tests |
|---|---|---|
| `truck_sk` | SHA2 surrogate key. | not_null, unique |
| `truck_id` | Natural key from source. | not_null, unique |
| `vin` | Vehicle identification number. | matches_regex `^[A-HJ-NPR-Z0-9]{17}$` |
| `fuel_type` | Fuel type. | accepted_values (Diesel) |
| `truck_status` | Operational status. | accepted_values (Active, Inactive, Maintenance) |

### dim_trailers

Gold trailers dimension. One row per trailer_id, plus the UNKNOWN
member row.

| Column | Description | Tests |
|---|---|---|
| `trailer_sk` | SHA2 surrogate key. | not_null, unique |
| `trailer_id` | Natural key from source. | not_null, unique |
| `trailer_type` | Trailer functional type. | accepted_values (Dry Van, Refrigerated) |
| `trailer_status` | Trailer status. | accepted_values (Active) |

### dim_date

Gold date dimension spine 2020 to 2030. Rows beyond today are by
design, which is why `full_date` only has a warn severity
`no_future_dates` test.

| Column | Description | Tests |
|---|---|---|
| `date_key` | Integer key in `yyyyMMdd` form. | not_null, unique |
| `full_date` | The calendar date. | not_null, no_future_dates (warn) |

The model also carries untested calendar attributes: `year`, `quarter`,
`month`, `day`, `day_of_week`, `week_of_year`, `day_name`, `month_name`,
and `is_weekend`.

### fact_loads

Gold fact loads at load grain with customer and route surrogate keys.

| Column | Description | Tests |
|---|---|---|
| `load_id` | Primary key, one row per load. | not_null, unique |
| `customer_sk` | Customer dimension key. | relationships to dim_customers.customer_sk |
| `customer_id` | Natural key kept for traceability. | relationships to dim_customers.customer_id (warn), no_orphan_rows (warn) |
| `route_sk` | Route dimension key. | relationships to dim_routes.route_sk (warn) |
| `load_date_key` | Date dimension key of load_date. | relationships to dim_date.date_key |
| `load_type` | Load categorization. | accepted_values (Dry Van, Refrigerated) |
| `load_status` | Load lifecycle status. | accepted_values (Completed) |
| `booking_type` | Booking channel. | accepted_values (Contract, Dedicated, Spot) |
| `total_charge` | Revenue plus fuel surcharge plus accessorial charges, nulls treated as zero. | accepted_range 0 to 200000 |
| `is_unmatched_customer` | True when the customer fell back to the UNKNOWN member. | not_null, accepted_values |
| `is_unmatched_route` | True when the route fell back to the UNKNOWN member. | not_null, accepted_values |

Untested pass through columns: `route_id`, `load_date`, `weight_lbs`,
`pieces`, `revenue`, `fuel_surcharge`, `accessorial_charges`,
`updated_at`.

### fact_trips

Gold fact trips at trip grain with driver truck trailer keys and
performance measures.

| Column | Description | Tests |
|---|---|---|
| `trip_id` | Primary key, one row per trip. | not_null, unique |
| `load_id` | Link to fact_loads. | relationships to fact_loads.load_id (warn) |
| `driver_sk` | Driver dimension key. | relationships to dim_drivers.driver_sk (warn) |
| `truck_sk` | Truck dimension key. | relationships to dim_trucks.truck_sk (warn) |
| `trailer_sk` | Trailer dimension key. | relationships to dim_trailers.trailer_sk (warn) |
| `dispatch_date_key` | Date dimension key of dispatch_date. | relationships to dim_date.date_key |
| `trip_status` | Trip lifecycle status. | accepted_values (Completed) |
| `is_unassigned` | True when driver or truck fell back to the UNKNOWN member. | accepted_values |

Untested pass through columns: `driver_id`, `truck_id`, `trailer_id`,
`dispatch_date`, `actual_distance_miles`, `actual_duration_hours`,
`fuel_gallons_used`, `average_mpg`, `idle_time_hours`, `avg_speed_mph`
(distance divided by duration), `updated_at`.

### fact_fuel_purchases

Gold fact fuel purchases with truck and driver keys and cost measures.

| Column | Description | Tests |
|---|---|---|
| `fuel_purchase_id` | Primary key, one row per purchase. | not_null, unique |
| `truck_sk` | Truck dimension key. | relationships to dim_trucks.truck_sk (warn) |
| `driver_sk` | Driver dimension key. | relationships to dim_drivers.driver_sk (warn) |
| `purchase_date_key` | Date dimension key of purchase_date. | relationships to dim_date.date_key |
| `is_unmatched_fuel_card` | True when the fuel card could not be matched. | accepted_values |

### fact_delivery_events

Gold fact delivery events at event grain with facility key and timing
measures.

| Column | Description | Tests |
|---|---|---|
| `event_id` | Primary key, one row per event. | not_null, unique |
| `load_id` | Link to fact_loads. | relationships to fact_loads.load_id |
| `trip_id` | Link to fact_trips. | relationships to fact_trips.trip_id (warn) |
| `facility_sk` | Facility dimension key. | relationships to dim_facilities.facility_sk (warn) |
| `event_type` | Delivery milestone type. | accepted_values (Delivery, Pickup) |
| `on_time_flag` | On time performance flag. | accepted_values (True, False) |
| `delay_minutes` | Actual minus scheduled, negative when early. | accepted_range -1440 to 1440 |
| `on_time_int` | Integer 1 or 0 form of on_time_flag for aggregation. | accepted_values (0, 1) |

Untested pass through columns: `facility_id`, `scheduled_datetime`,
`actual_datetime`, `detention_minutes`, `location_city`,
`location_state`, `updated_at`, `event_ts`.

### fact_maintenance_records

Gold fact maintenance records with truck key and cost downtime measures.

| Column | Description | Tests |
|---|---|---|
| `maintenance_id` | Primary key, one row per maintenance event. | not_null, unique |
| `truck_sk` | Truck dimension key. | relationships to dim_trucks.truck_sk |
| `maintenance_date_key` | Date dimension key of maintenance_date. | relationships to dim_date.date_key |
| `maintenance_type` | Category of maintenance. | accepted_values (Brake, Engine, Inspection, Preventive, Repair, Tire, Transmission) |

### fact_safety_incidents

Gold fact safety incidents with driver truck keys and cost measures.
Empty strings handled via NULLIF in silver.

| Column | Description | Tests |
|---|---|---|
| `incident_id` | Primary key, one row per incident. | not_null, unique |
| `driver_sk` | Driver dimension key. | relationships to dim_drivers.driver_sk (warn) |
| `truck_sk` | Truck dimension key. | relationships to dim_trucks.truck_sk (warn) |
| `incident_date_key` | Date dimension key of incident_date. | relationships to dim_date.date_key |
| `incident_type` | Category of safety incident. | accepted_values (Accident, Customer Complaint, DOT Violation, Equipment Damage, Moving Violation) |

### fact_operations

Gold operations aggregated fact by ops_date joining trips loads fuel
maintenance safety daily.

| Column | Description | Tests |
|---|---|---|
| `ops_date_key` | Primary key, the date of the daily aggregate in `yyyyMMdd` form, one row per day. | not_null, unique, relationships to dim_date.date_key |
| `ops_date` | The calendar date of the aggregate row. | not_null, no_future_dates |

Untested measures, all zero filled for days without activity in the
source: `trip_count`, `total_distance_miles`, `total_fuel_gallons`,
`avg_mpg`, `total_idle_hours`, `load_count`, `total_revenue`,
`total_weight_lbs`, `total_pieces`, `total_fuel_gallons_purchased`,
`total_fuel_cost`, `total_maintenance_cost`, `total_downtime_hours`,
`incident_count`, `total_claim_amount`. The `avg_mpg` is the weighted
average, total miles divided by total gallons, not the average of per
trip ratios.

## Serving mart

`publish_gold_to_postgres` copies all fourteen gold tables into a
`gold` schema (configurable through `POSTGRES_SCHEMA_GOLD`) inside the
`freightlake_mart` database, same table names, same columns, full
overwrite per table since gold materializes as tables. The indexes from
`sql/serving_mart/` are applied on top. Column shape is identical to
gold, so the tables above serve as the mart reference too.
`fact_operations` is the freshness signal for the mart: the latest
`ops_date` with any pipeline activity.
