-- FreightLake — create 3 logical DBs and users for Phase 2
-- This runs as superuser postgres on first init of postgres_data volume.
-- Idempotent via DO blocks.

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'freightlake_oltp_user') THEN
    CREATE ROLE freightlake_oltp_user WITH LOGIN PASSWORD 'changeme';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'freightlake_mart_user') THEN
    CREATE ROLE freightlake_mart_user WITH LOGIN PASSWORD 'changeme';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
    CREATE ROLE airflow WITH LOGIN PASSWORD 'admin' CREATEDB;
  END IF;
END $$;

SELECT 'CREATE DATABASE freightlake_oltp OWNER freightlake_oltp_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'freightlake_oltp')\gexec
SELECT 'CREATE DATABASE freightlake_mart OWNER freightlake_mart_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'freightlake_mart')\gexec
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

GRANT ALL PRIVILEGES ON DATABASE freightlake_oltp TO freightlake_oltp_user;
GRANT ALL PRIVILEGES ON DATABASE freightlake_mart TO freightlake_mart_user;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
