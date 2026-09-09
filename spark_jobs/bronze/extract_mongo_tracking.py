"""Bronze extract — Mongo tracking feed via watermark incremental."""

from __future__ import annotations

from spark_jobs.utils.logger import get_logger

logger = get_logger(__name__, "bronze")

COLLECTIONS = ["delivery_events", "safety_incidents", "maintenance_records"]


def extract_collection(name: str) -> None:
    logger.info("Extracting mongo collection=%s watermark event_ts", name)
    # Real job:
    # df = spark.read.format("mongodb").option("uri", mongo_uri()).option("collection", name).load()
    # filtered = df.filter(f"event_ts > '{watermark}'")
    # filtered.write.format("delta").mode("append").saveAsTable(f"freightlake.bronze.{name}")


def main() -> None:
    logger.info("Bronze Mongo extract start collections=%s", COLLECTIONS)
    for c in COLLECTIONS:
        extract_collection(c)
    logger.info("Bronze Mongo extract complete")


if __name__ == "__main__":
    main()
