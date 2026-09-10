-- FreightLake serving mart — star schema (Postgres)
-- Mirrors gold Delta star schema for BI. All facts partitioned by date if row counts justify it.

CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.dim_customer (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(200),
    customer_type VARCHAR(50),
    account_status VARCHAR(20),
    contract_start_date DATE,
    annual_revenue_potential BIGINT,
    credit_terms_days INT,
    primary_freight_type VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS mart.dim_driver (
    driver_sk VARCHAR(64) PRIMARY KEY,
    driver_id VARCHAR(20) NOT NULL,
    driver_name VARCHAR(200),
    employment_status VARCHAR(20),
    region VARCHAR(50),
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ NOT NULL,
    is_current BOOLEAN NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dim_driver_id ON mart.dim_driver (driver_id);
CREATE INDEX IF NOT EXISTS idx_dim_driver_current ON mart.dim_driver (is_current);

CREATE TABLE IF NOT EXISTS mart.dim_vehicle (
    vehicle_sk VARCHAR(64) PRIMARY KEY,
    truck_id VARCHAR(20) NOT NULL,
    make VARCHAR(100),
    model_year BIGINT,
    status VARCHAR(20),
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ NOT NULL,
    is_current BOOLEAN NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dim_vehicle_id ON mart.dim_vehicle (truck_id);
CREATE INDEX IF NOT EXISTS idx_dim_vehicle_current ON mart.dim_vehicle (is_current);

CREATE TABLE IF NOT EXISTS mart.dim_warehouse (
    warehouse_id VARCHAR(20) PRIMARY KEY,
    warehouse_name VARCHAR(200),
    warehouse_type VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(10),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    dock_doors INT,
    operating_hours VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS mart.dim_route (
    route_id VARCHAR(20) PRIMARY KEY,
    origin_city VARCHAR(100),
    origin_state VARCHAR(10),
    destination_city VARCHAR(100),
    destination_state VARCHAR(10),
    distance_miles INT,
    rate_per_mile DOUBLE PRECISION,
    fuel_surcharge_rate DOUBLE PRECISION,
    typical_transit_days INT
);

CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_id INT PRIMARY KEY,
    date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT,
    month INT NOT NULL,
    day INT,
    day_of_week INT,
    week_of_year INT,
    day_name VARCHAR(20),
    month_name VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS mart.fct_orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) REFERENCES mart.dim_customer (customer_id),
    route_id VARCHAR(20) REFERENCES mart.dim_route (route_id),
    load_date DATE,
    date_id INT REFERENCES mart.dim_date(date_id),
    load_type VARCHAR(50),
    weight_lbs BIGINT,
    pieces BIGINT,
    revenue DOUBLE PRECISION,
    fuel_surcharge DOUBLE PRECISION,
    accessorial_charges BIGINT,
    load_status VARCHAR(20),
    booking_type VARCHAR(20),
    updated_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_fct_orders_customer ON mart.fct_orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_fct_orders_route ON mart.fct_orders (route_id);
CREATE INDEX IF NOT EXISTS idx_fct_orders_date ON mart.fct_orders (load_date);

CREATE TABLE IF NOT EXISTS mart.fct_shipments (
    shipment_id VARCHAR(20) PRIMARY KEY,
    order_id VARCHAR(20) REFERENCES mart.fct_orders (order_id),
    driver_id VARCHAR(20),
    truck_id VARCHAR(20),
    trailer_id VARCHAR(20),
    ship_date DATE,
    date_id INT REFERENCES mart.dim_date(date_id),
    distance_miles BIGINT,
    duration_hours DOUBLE PRECISION,
    fuel_gallons_used DOUBLE PRECISION,
    average_mpg DOUBLE PRECISION,
    idle_time_hours DOUBLE PRECISION,
    trip_status VARCHAR(20),
    updated_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_fct_shipments_order ON mart.fct_shipments (order_id);
CREATE INDEX IF NOT EXISTS idx_fct_shipments_driver ON mart.fct_shipments (driver_id);
CREATE INDEX IF NOT EXISTS idx_fct_shipments_vehicle ON mart.fct_shipments (truck_id);

CREATE TABLE IF NOT EXISTS mart.fct_deliveries (
    delivery_id VARCHAR(20) PRIMARY KEY,
    order_id VARCHAR(20) REFERENCES mart.fct_orders (order_id),
    shipment_id VARCHAR(20) REFERENCES mart.fct_shipments (shipment_id),
    warehouse_id VARCHAR(20) REFERENCES mart.dim_warehouse (warehouse_id),
    delivery_ts TIMESTAMPTZ,
    date_id INT REFERENCES mart.dim_date(date_id),
    event_type VARCHAR(50),
    detention_minutes INT,
    is_on_time BOOLEAN,
    scheduled_datetime TIMESTAMPTZ,
    actual_datetime TIMESTAMPTZ,
    location_city VARCHAR(100),
    location_state VARCHAR(10)
);
CREATE INDEX IF NOT EXISTS idx_fct_deliveries_order ON mart.fct_deliveries (order_id);
CREATE INDEX IF NOT EXISTS idx_fct_deliveries_shipment ON mart.fct_deliveries (shipment_id);
CREATE INDEX IF NOT EXISTS idx_fct_deliveries_warehouse ON mart.fct_deliveries (warehouse_id);
CREATE INDEX IF NOT EXISTS idx_fct_deliveries_ts ON mart.fct_deliveries (delivery_ts);
