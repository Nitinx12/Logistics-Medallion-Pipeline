-- FreightLake OLTP — customers
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    customer_type VARCHAR(50),
    credit_terms_days INT,
    primary_freight_type VARCHAR(100),
    account_status VARCHAR(20),
    contract_start_date DATE,
    annual_revenue_potential DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_customers_updated_at ON customers (updated_at);
