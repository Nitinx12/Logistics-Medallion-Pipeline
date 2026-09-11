DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'freightlake_oltp_user') THEN
    CREATE ROLE freightlake_oltp_user WITH LOGIN PASSWORD 'changeme';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'freightlake_mart_user') THEN
    CREATE ROLE freightlake_mart_user WITH LOGIN PASSWORD 'changeme';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
    -- Fixed password 'airflow' matches AIRFLOW__DATABASE__SQL_ALCHEMY_CONN in compose.yml
    CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow' CREATEDB;
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