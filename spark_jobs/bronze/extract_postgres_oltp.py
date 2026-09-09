"""Bronze extract — Postgres OLTP via watermark incremental MERGE."""

from __future__ import annotations

import os

import psycopg2
from dotenv import load_dotenv

from spark_jobs.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__, "bronze")

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

WATERMARK_SQL = """
SELECT watermark_ts FROM freightlake.bronze.etl_watermark
WHERE source_system='postgres_oltp' AND source_table=%s
"""

MERGE_SQL_TEMPLATE = """
MERGE INTO freightlake.bronze.{table} AS tgt
USING (SELECT * FROM temp_{table}) AS src
ON tgt.{pk} = src.{pk}
WHEN MATCHED AND src.updated_at > tgt.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""


def watermark_for(table: str, cur) -> str:
    cur.execute(WATERMARK_SQL, (table,))
    row = cur.fetchone()
    return str(row[0]) if row else "1970-01-01 00:00:00"


def extract_table(table: str, pk: str = "id") -> None:
    # Placeholder: real job uses PySpark JDBC with watermark filtering
    # and Delta MERGE. This stub logs intent for CI and local runs
    # without requiring a Databricks cluster.
    logger.info("Extracting postgres table=%s pk=%s", table, pk)
    # Example JDBC read would be:
    # df = spark.read.format("jdbc").option("url", pg_oltp_url()).option("dbtable",
    #   f"(SELECT * FROM {table} WHERE updated_at > '{watermark}')").load()
    # df.write.format("delta").mode("append").saveAsTable(f"freightlake.bronze.{table}")


def main() -> None:
    logger.info("Bronze Postgres extract start tables=%s", TABLES)
    for t in TABLES:
        extract_table(t)
    logger.info("Bronze Postgres extract complete")


if __name__ == "__main__":
    main()
