CREATE TABLE IF NOT EXISTS drivers (
    driver_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    hire_date DATE,
    termination_date DATE,
    license_number VARCHAR(50),
    license_state VARCHAR(10),
    date_of_birth DATE,
    home_terminal VARCHAR(50),
    employment_status VARCHAR(20),
    cdl_class VARCHAR(10),
    years_experience INT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
