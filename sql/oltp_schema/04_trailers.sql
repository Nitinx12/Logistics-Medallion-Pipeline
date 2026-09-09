CREATE TABLE IF NOT EXISTS trailers (
  trailer_id VARCHAR(20) PRIMARY KEY,
  trailer_number VARCHAR(20),
  trailer_type VARCHAR(50),
  length_feet INT,
  model_year INT,
  vin VARCHAR(50),
  acquisition_date DATE,
  status VARCHAR(20),
  current_location VARCHAR(100),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_trailers_updated_at ON trailers (updated_at);
