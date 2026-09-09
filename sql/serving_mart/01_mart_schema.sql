-- FreightLake serving mart — star schema
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.dim_customer (
  customer_id VARCHAR(20) PRIMARY KEY,
  customer_name VARCHAR(200),
  customer_type VARCHAR(50),
  account_status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS mart.fct_orders (
  order_id VARCHAR(20) PRIMARY KEY,
  customer_id VARCHAR(20) REFERENCES mart.dim_customer(customer_id),
  route_id VARCHAR(20),
  load_date DATE,
  revenue DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_fct_orders_customer ON mart.fct_orders (customer_id);
