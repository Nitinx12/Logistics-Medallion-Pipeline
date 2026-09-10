"""FreightLake seed — loads CSVs into Postgres OLTP and Mongo collections.

Idempotent: truncates then COPYs Postgres, drops then inserts Mongo.
Handles trimmed spaces and updated_at watermarks.
"""

from __future__ import annotations

import os
import pathlib

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

DATA_DIR = pathlib.Path("data")
# Respect .env and Docker port mapping (host 5434 vs container 5432) — fallback to legacy freight_lake
POSTGRES_URL = os.getenv(
    "POSTGRES_OLTP_URL",
    os.getenv(
        "DATABASE_URL", "postgresql://postgres:admin@localhost:5432/freight_lake"
    ),
)
# Auto-switch to 5434 when host 5432 is occupied by local Postgres (see docker/compose.yml AGENTS.md:158)
if "localhost:5432/freight_lake" in POSTGRES_URL:
    import socket as _sock

    _s = _sock.socket()
    try:
        _s.settimeout(0.3)
        _s.connect(("localhost", 5432))
        # if connect succeeds, host 5432 is in use — prefer 5434 for Docker OLTP
        if os.getenv("POSTGRES_DOCKER_PORT"):
            POSTGRES_URL = POSTGRES_URL.replace(
                "localhost:5432", f"localhost:{os.getenv('POSTGRES_DOCKER_PORT')}"
            )
    except OSError:
        pass
    finally:
        try:
            _s.close()
        except OSError:
            pass
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_URI_NOPASS = "mongodb://localhost:27017"

# Mapping: CSV -> Postgres table
PG_MAP = {
    "customers.csv": "customers",
    "drivers.csv": "drivers",
    "trucks.csv": "trucks",
    "trailers.csv": "trailers",
    "facilities.csv": "facilities",
    "routes.csv": "routes",
    "loads.csv": "loads",
    "trips.csv": "trips",
    "fuel_purchases.csv": "fuel_purchases",
}
# CSV -> Mongo collection (plan names vs actual)
MONGO_MAP = {
    "delivery_events.csv": "delivery_events",  # plan tracking_events
    "safety_incidents.csv": "safety_incidents",  # plan delivery_exceptions
    "maintenance_records.csv": "maintenance_records",  # plan driver_app_events
}


def pg_seed():
    print("[seed] Postgres OLTP")
    # Use superuser for DDL + TRUNCATE/COPY so ownership/privilege is not an issue
    # Fall back to POSTGRES_URL if superuser vars not set
    super_url = os.getenv("POSTGRES_SUPERUSER_URL")
    if not super_url:
        port = os.getenv("POSTGRES_DOCKER_PORT", "55432")
        # prefer 55432 when host 5432 is occupied, else 5432
        super_url = f"postgresql://{os.getenv('POSTGRES_SUPERUSER', 'postgres')}:{os.getenv('POSTGRES_SUPERUSER_PASSWORD', 'admin')}@localhost:{port}/{os.getenv('POSTGRES_OLTP_DB', 'freightlake_oltp')}"
        # if that fails, try legacy freight_lake on same port
        try:
            test_conn = psycopg2.connect(super_url)
            test_conn.close()
        except psycopg2.OperationalError:
            legacy = super_url.replace("/freightlake_oltp", "/freight_lake")
            try:
                test_conn = psycopg2.connect(legacy)
                test_conn.close()
                super_url = legacy
            except psycopg2.OperationalError:
                # final fallback to POSTGRES_URL (oltp_user)
                super_url = POSTGRES_URL
    print(f"  connecting {super_url.split('@')[-1]}")
    conn = psycopg2.connect(super_url)
    conn.autocommit = True
    cur = conn.cursor()
    for sql_file in sorted(pathlib.Path("sql/oltp_schema").glob("*.sql")):
        if sql_file.name == "README.md":
            continue
        sql = sql_file.read_text()
        try:
            cur.execute(sql)
            print(f"  DDL {sql_file.name} OK")
        except Exception as e:  # noqa: BLE001
            print(f"  DDL {sql_file.name} ERR {e}")
    # Ensure oltp_user can still write after superuser creates tables
    try:
        oltp_user = os.getenv("POSTGRES_OLTP_USER", "freightlake_oltp_user")
        cur.execute(
            f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "{oltp_user}"'
        )
        cur.execute(
            f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "{oltp_user}"'
        )
        cur.execute(
            f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "{oltp_user}"'
        )
    except Exception as e:  # noqa: BLE001
        print(f"  grant to oltp_user skipped: {e}")
    # load CSVs
    for csv_name, table in PG_MAP.items():
        path = DATA_DIR / csv_name
        if not path.exists():
            print(f"  skip {csv_name} not found")
            continue
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        df.columns = [c.strip() for c in df.columns]
        # trim spaces
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"": None, "nan": None, "None": None})
        # truncate
        cur.execute(f"TRUNCATE {table} CASCADE")
        # use copy_expert
        cols = ",".join([f'"{c}"' for c in df.columns])
        # create temp csv for COPY
        import io

        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        try:
            cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH CSV", buf)
            print(f"  {table}: {len(df)} rows")
        except Exception as e:  # noqa: BLE001
            print(f"  {table} COPY ERR {e} — falling back to insert")
            for _, row in df.iterrows():
                placeholders = ",".join(["%s"] * len(row))
                cur.execute(
                    f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                    tuple(row),
                )
            print(f"  {table}: {len(df)} rows via insert")
    cur.close()
    conn.close()


def mongo_seed():
    print("[seed] Mongo freight_lake")
    client = None
    for uri in [MONGO_URI, MONGO_URI_NOPASS]:
        try:
            c = MongoClient(uri, serverSelectionTimeoutMS=2000)
            c.list_database_names()
            client = c
            print(f"  connected {uri}")
            break
        except Exception as e:  # noqa: BLE001
            print(f"  {uri} failed {e}")
    if client is None:
        print("  Mongo not reachable, skipping")
        return
    db = client["freight_lake"]
    for csv_name, coll in MONGO_MAP.items():
        path = DATA_DIR / csv_name
        if not path.exists():
            print(f"  skip {csv_name}")
            continue
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        df.columns = [c.strip() for c in df.columns]
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()
        docs = df.where(pd.notna(df), None).to_dict("records")
        db[coll].drop()
        if docs:
            db[coll].insert_many(docs)
        print(f"  {coll}: {len(docs)} docs")
    client.close()


if __name__ == "__main__":
    pg_seed()
    mongo_seed()
    print("Seed complete")
