"""Bronze extract — Mongo tracking feed via PySpark MongoDB connector + MERGE."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs.utils.connection import mongo_uri
from spark_jobs.utils.engine import get_spark
from spark_jobs.utils.logger import get_logger

logger = get_logger(__name__, "bronze")

CATALOG = os.getenv("DATABRICKS_CATALOG", "freightlake")
SCHEMA = os.getenv("DATABRICKS_SCHEMA_BRONZE", "bronze")
PK_MAP: dict[str, str] = {
    "delivery_events": "event_id",
    "safety_incidents": "incident_id",
    "maintenance_records": "maintenance_id",
}
WM_COL_MAP: dict[str, str] = {
    "delivery_events": "event_ts",
    "safety_incidents": "event_ts",
    "maintenance_records": "event_ts",
}


def get_watermark(spark: SparkSession, coll: str) -> str:
    row = spark.sql(f"""
        SELECT CAST(watermark_ts AS STRING) AS wm
        FROM {CATALOG}.{SCHEMA}.etl_watermark
        WHERE source_system='mongo' AND source_table='{coll}'
    """).first()
    return str(row["wm"]) if row and row["wm"] else "1970-01-01T00:00:00"


def set_watermark(spark: SparkSession, coll: str, ts: str) -> None:
    spark.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.etl_watermark AS tgt
        USING (SELECT 'mongo' AS source_system, '{coll}' AS source_table,
                      CAST('{ts}' AS TIMESTAMP) AS watermark_ts,
                      current_timestamp() AS updated_at) AS src
        ON tgt.source_system = src.source_system
           AND tgt.source_table = src.source_table
        WHEN MATCHED THEN UPDATE SET
            tgt.watermark_ts = src.watermark_ts,
            tgt.updated_at   = src.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)


def extract_collection(spark: SparkSession, coll: str) -> None:
    pk = PK_MAP[coll]
    wm_col = WM_COL_MAP[coll]
    wm = get_watermark(spark, coll)
    uri = mongo_uri()
    # db name is last segment before query string
    db_name = uri.split("/")[-1].split("?")[0]
    df = (
        spark.read.format("mongodb")
        .option("connection.uri", uri)
        .option("database", db_name)
        .option("collection", coll)
        .option("pipeline", f'[{{"$match": {{"{wm_col}": {{"$gt": "{wm}"}}}}}}]')
        .load()
        .drop("_id")
    )
    if df.count() == 0:
        logger.info("  %s 0 docs after watermark %s", coll, wm)
        return
    df = df.withColumn("_loaded_at", F.current_timestamp())
    df.createOrReplaceTempView(f"temp_{coll}")
    spark.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.{coll} AS tgt
        USING temp_{coll} AS src
        ON tgt.{pk} = src.{pk}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    max_ts_row = df.agg(F.max(wm_col)).first()
    max_ts = str(max_ts_row[0]) if max_ts_row and max_ts_row[0] else wm
    set_watermark(spark, coll, max_ts)
    logger.info("  %s MERGE %s docs watermark -> %s", coll, df.count(), max_ts)


def main() -> None:
    spark = get_spark("FreightLake-Bronze-Mongo")
    for coll in PK_MAP:
        extract_collection(spark, coll)
    spark.stop()


if __name__ == "__main__":
    main()
