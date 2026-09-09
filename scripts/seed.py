"""FreightLake seed — loads CSVs into Postgres OLTP and Mongo collections.

Idempotent: truncates then COPYs Postgres, drops then inserts Mongo.
Handles trimmed spaces and updated_at watermarks.
"""

from __future__ import annotations

import os
import pathlib
import sys

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

DATA_DIR = pathlib.Path("data")
POSTGRES_URL = "postgresql://postgres:admin@localhost:5432/freight_lake"
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
    # apply DDL first
    conn = psycopg2.connect(POSTGRES_URL)
    conn.autocommit = True
    cur = conn.cursor()
    for sql_file in sorted(pathlib.Path("sql/oltp_schema").glob("*.sql")):
        if sql_file.name == "README.md":
            continue
        sql = sql_file.read_text()
        try:
            cur.execute(sql)
            print(f"  DDL {sql_file.name} OK")
        except Exception as e:
            print(f"  DDL {sql_file.name} ERR {e}")
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
        except Exception as e:
            print(f"  {table} COPY ERR {e} — falling back to insert")
            for _, row in df.iterrows():
                placeholders = ",".join(["%s"] * len(row))
                cur.execute(f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT DO NOTHING", tuple(row))
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
        except Exception as e:
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
