-- 01_example_customers.sql - placeholder OLTP example
-- Replace with real FreightLake OLTP DDL, kept as example so init loop has at least one file
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_name VARCHAR NOT NULL,
    customer_type VARCHAR,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
