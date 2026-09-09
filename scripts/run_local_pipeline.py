"""FreightLake local medallion pipeline — runnable without Databricks/Docker.

Reads from local Postgres freight_lake and Mongo freight_lake,
writes bronze/silver/gold as local Delta/Parquet under delta/,
and publishes gold star schema to Postgres mart (freightlake_mart or freight_lake mart schema).
Watermark logic and SCD2 are demonstrated with file based watermark.json.
"""

from __future__ import annotations

import json
import pathlib
import time
from datetime import UTC, datetime

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

import os

POSTGRES_URL = "postgresql://postgres:admin@localhost:5432/freight_lake"
POSTGRES_SUPER = "postgresql://postgres:admin@localhost:5432/postgres"
MART_URL = os.getenv(
    "POSTGRES_MART_URL", "postgresql://postgres:admin@localhost:5432/freightlake_mart"
)
# if mart db is freightlake_mart but local only has freight_lake, fallback to freight_lake with mart schema
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_URI_NOPASS = "mongodb://localhost:27017"
DELTA_ROOT = pathlib.Path("delta")
BRONZE = DELTA_ROOT / "bronze"
SILVER = DELTA_ROOT / "silver"
GOLD = DELTA_ROOT / "gold"
WATERMARK = pathlib.Path("watermarks.json")

TABLES = [
    "customers",
    "drivers",
    "trucks",
    "trailers",
    "facilities",
    "routes",
    "loads",
    "trips",
    "fuel_purchases",
]
MONGO_COLLS = ["delivery_events", "safety_incidents", "maintenance_records"]


def watermark_get(key: str) -> str:
    if WATERMARK.exists():
        try:
            return json.loads(WATERMARK.read_text()).get(key, "1970-01-01T00:00:00")
        except (json.JSONDecodeError, OSError):
            return "1970-01-01T00:00:00"
    return "1970-01-01T00:00:00"


def watermark_set(key: str, ts: str) -> None:
    data = json.loads(WATERMARK.read_text()) if WATERMARK.exists() else {}
    data[key] = ts
    WATERMARK.write_text(json.dumps(data, indent=2))


def pg_connect(url: str):
    return psycopg2.connect(url)


def ensure_mart():
    # ensure mart DB exists, else use freight_lake with schema mart
    try:
        conn = pg_connect(POSTGRES_SUPER)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname='freightlake_mart'")
        if not cur.fetchone():
            cur.execute("CREATE DATABASE freightlake_mart")
            print("Created database freightlake_mart")
        cur.close()
        conn.close()
        return "postgresql://postgres:admin@localhost:5432/freightlake_mart"
    except Exception as e:  # noqa: BLE001
        print(f"mart DB create skipped: {e}")
        return POSTGRES_URL


def bronze_postgres():
    print("\n=== BRONZE: Postgres OLTP ===")
    BRONZE.mkdir(parents=True, exist_ok=True)
    conn = pg_connect(
        POSTGRES_URL
        if "freight_lake" in POSTGRES_URL
        else POSTGRES_SUPER.replace("/postgres", "/freight_lake")
    )
    # fallback try
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
    except Exception:  # noqa: BLE001
        conn = pg_connect("postgresql://postgres:admin@localhost:5432/freight_lake")
    for tbl in TABLES:
        wm = watermark_get(f"pg:{tbl}")
        print(f"  {tbl} watermark {wm} -> ", end="")
        try:
            df = pd.read_sql(
                f"SELECT * FROM {tbl} WHERE updated_at > %s", conn, params=(wm,)
            )
            if df.empty:
                print("0 new rows")
                continue
            df["_loaded_at"] = datetime.now(UTC)
            # watermark advance
            max_ts = df["updated_at"].max()
            out = BRONZE / f"{tbl}.parquet"
            df.to_parquet(out, index=False)
            watermark_set(f"pg:{tbl}", str(max_ts))
            print(f"{len(df)} rows -> {out} watermark {max_ts}")
        except Exception as e:  # noqa: BLE001
            print(f"ERR {e}")
    conn.close()


def bronze_mongo():
    print("\n=== BRONZE: Mongo ===")
    BRONZE.mkdir(parents=True, exist_ok=True)
    # try auth then no auth
    for uri in [MONGO_URI, MONGO_URI_NOPASS]:
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.list_database_names()
            break
        except Exception:  # noqa: BLE001
            client = None
    if client is None:
        print("Mongo not reachable")
        return
    db = client["freight_lake"]
    for coll in MONGO_COLLS:
        wm = watermark_get(f"mongo:{coll}")
        print(f"  {coll} watermark {wm} -> ", end="")
        try:
            # event_ts or updated_at
            docs = list(
                db[coll].find(
                    {"$or": [{"event_ts": {"$gt": wm}}, {"updated_at": {"$gt": wm}}]},
                    {"_id": 0},
                )
            )
            if not docs:
                # fallback all
                docs = list(db[coll].find({}, {"_id": 0}))
                if not docs:
                    print("0 docs")
                    continue
            df = pd.DataFrame(docs)
            df["_loaded_at"] = datetime.now(UTC)
            out = BRONZE / f"{coll}.parquet"
            df.to_parquet(out, index=False)
            # watermark = max event_ts
            if "event_ts" in df.columns:
                max_ts = df["event_ts"].max()
                watermark_set(f"mongo:{coll}", str(max_ts))
            print(f"{len(df)} docs -> {out}")
        except Exception as e:  # noqa: BLE001
            print(f"ERR {e}")
    client.close()


def silver():
    print("\n=== SILVER: clean + dedup + SCD2 ===")
    SILVER.mkdir(parents=True, exist_ok=True)
    # customers -> dim_customer scd1
    if (BRONZE / "customers.parquet").exists():
        df = pd.read_parquet(BRONZE / "customers.parquet")
        df["customer_id"] = df["customer_id"].astype(str).str.strip()
        df = df.drop_duplicates(subset=["customer_id"])
        df.to_parquet(SILVER / "dim_customer.parquet", index=False)
        print(f"  dim_customer {len(df)}")
    # drivers SCD2
    if (BRONZE / "drivers.parquet").exists():
        df = pd.read_parquet(BRONZE / "drivers.parquet")
        df["driver_id"] = df["driver_id"].astype(str).str.strip()
        df["valid_from"] = pd.to_datetime(df["updated_at"])
        df = df.sort_values(["driver_id", "valid_from"])
        df["valid_to"] = df.groupby("driver_id")["valid_from"].shift(-1)
        df["is_current"] = df["valid_to"].isna()
        df["valid_to"] = df["valid_to"].fillna(pd.Timestamp("9999-12-31"))
        df["driver_sk"] = df["driver_id"] + "_" + df["valid_from"].astype(str)
        df.to_parquet(SILVER / "dim_driver.parquet", index=False)
        print(f"  dim_driver SCD2 {len(df)}")
    # trucks SCD2
    if (BRONZE / "trucks.parquet").exists():
        df = pd.read_parquet(BRONZE / "trucks.parquet")
        df["truck_id"] = df["truck_id"].astype(str).str.strip()
        df["valid_from"] = pd.to_datetime(df["updated_at"])
        df = df.sort_values(["truck_id", "valid_from"])
        df["valid_to"] = df.groupby("truck_id")["valid_from"].shift(-1)
        df["is_current"] = df["valid_to"].isna()
        df["valid_to"] = df["valid_to"].fillna(pd.Timestamp("9999-12-31"))
        df["vehicle_sk"] = df["truck_id"] + "_" + df["valid_from"].astype(str)
        df.to_parquet(SILVER / "dim_vehicle.parquet", index=False)
        print(f"  dim_vehicle SCD2 {len(df)}")
    for tbl in ["loads", "trips", "fuel_purchases", "routes", "facilities"]:
        p = BRONZE / f"{tbl}.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            # trim all string columns to fix join keys (OLTP has padded spaces)
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = (
                    df[col].astype(str).str.strip().replace({"nan": None, "None": None})
                )
            df = df.drop_duplicates()
            df.to_parquet(SILVER / f"stg_{tbl}.parquet", index=False)
            print(f"  stg_{tbl} {len(df)}")
    # delivery_events silver
    if (BRONZE / "delivery_events.parquet").exists():
        df = pd.read_parquet(BRONZE / "delivery_events.parquet")
        df.to_parquet(SILVER / "stg_delivery_events.parquet", index=False)
        print(f"  stg_delivery_events {len(df)}")


def gold():
    print("\n=== GOLD: star schema ===")
    GOLD.mkdir(parents=True, exist_ok=True)
    # dim_customer
    if (SILVER / "dim_customer.parquet").exists():
        pd.read_parquet(SILVER / "dim_customer.parquet").to_parquet(
            GOLD / "dim_customer.parquet", index=False
        )
        print("  dim_customer")
    if (SILVER / "dim_driver.parquet").exists():
        pd.read_parquet(SILVER / "dim_driver.parquet").to_parquet(
            GOLD / "dim_driver.parquet", index=False
        )
        print("  dim_driver")
    if (SILVER / "dim_vehicle.parquet").exists():
        pd.read_parquet(SILVER / "dim_vehicle.parquet").to_parquet(
            GOLD / "dim_vehicle.parquet", index=False
        )
        print("  dim_vehicle")
    # fct_orders from loads
    if (SILVER / "stg_loads.parquet").exists():
        df = pd.read_parquet(SILVER / "stg_loads.parquet")
        df.rename(columns={"load_id": "order_id"}, inplace=True)
        df.to_parquet(GOLD / "fct_orders.parquet", index=False)
        print(f"  fct_orders {len(df)}")
    # fct_shipments from trips + delivery_events join
    if (SILVER / "stg_trips.parquet").exists():
        df = pd.read_parquet(SILVER / "stg_trips.parquet")
        df.to_parquet(GOLD / "fct_shipments.parquet", index=False)
        print(f"  fct_shipments {len(df)}")
    if (SILVER / "stg_delivery_events.parquet").exists():
        df = pd.read_parquet(SILVER / "stg_delivery_events.parquet")
        df.to_parquet(GOLD / "fct_deliveries.parquet", index=False)
        print(f"  fct_deliveries {len(df)}")
    # dim_warehouse and dim_route
    if (SILVER / "stg_facilities.parquet").exists():
        df = pd.read_parquet(SILVER / "stg_facilities.parquet")
        df.rename(
            columns={"facility_id": "warehouse_id", "facility_name": "warehouse_name"},
            inplace=True,
        )
        df.to_parquet(GOLD / "dim_warehouse.parquet", index=False)
        print(f"  dim_warehouse {len(df)}")
    if (SILVER / "stg_routes.parquet").exists():
        df = pd.read_parquet(SILVER / "stg_routes.parquet")
        df.to_parquet(GOLD / "dim_route.parquet", index=False)
        print(f"  dim_route {len(df)}")
    # dim_date
    dates = pd.date_range("2022-01-01", "2026-12-31", freq="D")
    dim_date = pd.DataFrame(
        {
            "date": dates,
            "date_id": dates.strftime("%Y%m%d"),
            "year": dates.year,
            "month": dates.month,
        }
    )
    dim_date.to_parquet(GOLD / "dim_date.parquet", index=False)
    print(f"  dim_date {len(dim_date)}")


def publish():
    print("\n=== PUBLISH: gold to Postgres mart ===")
    mart_url = ensure_mart()
    conn = pg_connect(mart_url)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS mart")
    for tbl in [
        "dim_customer",
        "dim_driver",
        "dim_vehicle",
        "dim_warehouse",
        "dim_route",
        "dim_date",
        "fct_orders",
        "fct_shipments",
        "fct_deliveries",
    ]:
        p = GOLD / f"{tbl}.parquet"
        if not p.exists():
            print(f"  skip {tbl} no file")
            continue
        df = pd.read_parquet(p)
        # create table via pandas dtype mapping
        # simple: drop and recreate
        cur.execute(f"DROP TABLE IF EXISTS mart.{tbl} CASCADE")
        # use pandas to_sql via psycopg2
        # create via sql
        cols = []
        for col, dtype in zip(df.columns, df.dtypes):
            if "int" in str(dtype):
                typ = "BIGINT"
            elif "float" in str(dtype):
                typ = "DOUBLE PRECISION"
            elif "datetime" in str(dtype):
                typ = "TIMESTAMPTZ"
            elif "bool" in str(dtype):
                typ = "BOOLEAN"
            else:
                typ = "TEXT"
            cols.append(f'"{col}" {typ}')
        cur.execute(f"CREATE TABLE mart.{tbl} ({', '.join(cols)})")
        # bulk insert via execute_values
        import psycopg2.extras

        rows = [
            tuple(None if pd.isna(x) else x for x in row)
            for row in df.itertuples(index=False)
        ]
        if rows:
            psycopg2.extras.execute_values(
                cur, f"INSERT INTO mart.{tbl} VALUES %s", rows, page_size=5000
            )
        print(f"  mart.{tbl} {len(df)} rows")
    cur.close()
    conn.close()


def main():
    start = time.time()
    print(f"FreightLake local pipeline start {datetime.now(UTC)}")
    bronze_postgres()
    bronze_mongo()
    silver()
    gold()
    publish()
    print(f"\nPipeline complete in {time.time() - start:.1f}s")
    print(
        "Verify: psql freightlake_mart -c 'SELECT tablename FROM pg_tables WHERE schemaname=''mart'''"
    )
    print("Delta local: delta/bronze, delta/silver, delta/gold")


if __name__ == "__main__":
    main()
