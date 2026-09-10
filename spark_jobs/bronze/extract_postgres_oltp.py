"""Bronze extract — Postgres OLTP via PySpark JDBC watermark MERGE."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs.utils.connection import pg_oltp_url
from spark_jobs.utils.engine import get_spark
from spark_jobs.utils.logger import get_logger

logger = get_logger(__name__, "bronze")

CATALOG = os.getenv("DATABRICKS_CATALOG", "freightlake")
SCHEMA = os.getenv("DATABRICKS_SCHEMA_BRONZE", "bronze")
PK_MAP: dict[str, str] = {
    "customers": "customer_id",
    "drivers": "driver_id",
    "trucks": "truck_id",
    "trailers": "trailer_id",
    "facilities": "facility_id",
    "routes": "route_id",
    "loads": "load_id",
    "trips": "trip_id",
    "fuel_purchases": "fuel_purchase_id",
}


def get_watermark(spark: SparkSession, table: str) -> str:
    row = spark.sql(f"""
        SELECT CAST(watermark_ts AS STRING) AS wm
        FROM {CATALOG}.{SCHEMA}.etl_watermark
        WHERE source_system='postgres_oltp' AND source_table='{table}'
    """).first()
    return str(row["wm"]) if row and row["wm"] else "1970-01-01T00:00:00"


def set_watermark(spark: SparkSession, table: str, ts: str) -> None:
    spark.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.etl_watermark AS tgt
        USING (SELECT 'postgres_oltp' AS source_system,
                      '{table}' AS source_table,
                      CAST('{ts}' AS TIMESTAMP) AS watermark_ts,
                      current_timestamp() AS updated_at) AS src
        ON tgt.source_system = src.source_system
           AND tgt.source_table = src.source_table
        WHEN MATCHED THEN UPDATE SET
            tgt.watermark_ts = src.watermark_ts,
            tgt.updated_at   = src.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)


def extract_table(spark: SparkSession, table: str) -> None:
    pk = PK_MAP[table]
    wm = get_watermark(spark, table)
    url = pg_oltp_url().replace("postgresql://", "jdbc:postgresql://")
    df = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", f"(SELECT * FROM {table} WHERE updated_at > '{wm}') t")
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    if df.count() == 0:
        logger.info("  %s 0 rows after watermark %s", table, wm)
        return
    df = df.withColumn("_loaded_at", F.current_timestamp())
    df.createOrReplaceTempView(f"temp_{table}")
    spark.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.{table} AS tgt
        USING temp_{table} AS src
        ON tgt.{pk} = src.{pk}
        WHEN MATCHED AND src.updated_at > tgt.updated_at
            THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    max_ts_row = df.agg(F.max("updated_at")).first()
    max_ts = str(max_ts_row[0]) if max_ts_row and max_ts_row[0] else wm
    set_watermark(spark, table, max_ts)
    logger.info("  %s MERGE %s rows watermark -> %s", table, df.count(), max_ts)


def main() -> None:
    spark = get_spark("FreightLake-Bronze-Postgres")
    for table in PK_MAP:
        extract_table(spark, table)
    spark.stop()


if __name__ == "__main__":
    main()
