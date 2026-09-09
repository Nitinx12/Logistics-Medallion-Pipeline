"""Publish gold Delta tables to Postgres serving mart."""

from __future__ import annotations

from spark_jobs.utils.logger import get_logger

logger = get_logger(__name__, "publish")

GOLD_TABLES = [
    "dim_customer",
    "dim_driver",
    "dim_vehicle",
    "dim_warehouse",
    "dim_route",
    "dim_date",
    "fct_orders",
    "fct_shipments",
    "fct_deliveries",
]


def publish(table: str) -> None:
    logger.info("Publishing gold table=%s to postgres mart", table)
    # Real: spark.table(f"freightlake.gold.{table}").write.jdbc(pg_mart_url(), table, mode="overwrite")


def main() -> None:
    logger.info("Publish start tables=%s", GOLD_TABLES)
    for t in GOLD_TABLES:
        publish(t)
    logger.info("Publish complete")


if __name__ == "__main__":
    main()
