-- FreightLake serving mart — star schema (Postgres)
-- Mirrors gold Delta star schema for BI. All facts partitioned by date if row counts justify it.

CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.dim_customer (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(200),
    customer_type VARCHAR(50),
    account_status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS mart.dim_driver (
    driver_id VARCHAR(20) PRIMARY KEY,
    driver_name VARCHAR(200),
    employment_status VARCHAR(20),
    region VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS mart.dim_vehicle (
    vehicle_id VARCHAR(20) PRIMARY KEY,
    make VARCHAR(100),
    status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS mart.dim_warehouse (
    warehouse_id VARCHAR(20) PRIMARY KEY,
    warehouse_name VARCHAR(200),
    warehouse_type VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS mart.dim_route (
    route_id VARCHAR(20) PRIMARY KEY,
    origin_city VARCHAR(100),
    origin_state VARCHAR(10),
    destination_city VARCHAR(100),
    destination_state VARCHAR(10),
    distance_miles DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_id INT PRIMARY KEY,
    date DATE,
    year INT,
    month INT
);

CREATE TABLE IF NOT EXISTS mart.fct_orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) REFERENCES mart.dim_customer (customer_id),
    route_id VARCHAR(20) REFERENCES mart.dim_route (route_id),
    load_date DATE,
    revenue DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_fct_orders_customer ON mart.fct_orders (
    customer_id
);
CREATE INDEX IF NOT EXISTS idx_fct_orders_route ON mart.fct_orders (route_id);
CREATE INDEX IF NOT EXISTS idx_fct_orders_date ON mart.fct_orders (load_date);

CREATE TABLE IF NOT EXISTS mart.fct_shipments (
    shipment_id VARCHAR(20) PRIMARY KEY,
    order_id VARCHAR(20) REFERENCES mart.fct_orders (order_id),
    driver_id VARCHAR(20) REFERENCES mart.dim_driver (driver_id),
    vehicle_id VARCHAR(20) REFERENCES mart.dim_vehicle (vehicle_id),
    ship_date DATE,
    distance_miles DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_fct_shipments_order ON mart.fct_shipments (
    order_id
);

CREATE TABLE IF NOT EXISTS mart.fct_deliveries (
    delivery_id VARCHAR(20) PRIMARY KEY,
    order_id VARCHAR(20) REFERENCES mart.fct_orders (order_id),
    shipment_id VARCHAR(20) REFERENCES mart.fct_shipments (shipment_id),
    delivery_ts TIMESTAMPTZ,
    status VARCHAR(50),
    is_on_time BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_fct_deliveries_order ON mart.fct_deliveries (
    order_id
);
