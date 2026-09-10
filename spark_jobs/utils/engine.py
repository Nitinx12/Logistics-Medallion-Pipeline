"""Spark session factory for FreightLake."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "FreightLake") -> SparkSession:
    # Packages needed for local runs: delta, postgres jdbc, mongo connector.
    # On Databricks these are provided via cluster libraries, so we only add them
    # when not already on classpath. Use env var to override.
    packages = os.getenv(
        "SPARK_PACKAGES",
        "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
    )
    # Try Delta-enabled session first; fallback to plain Spark if Delta jars not present
    try:
        builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        )
        # Only set packages if not on Databricks (DBR sets this)
        if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
            builder = builder.config("spark.jars.packages", packages)
        return builder.getOrCreate()
    except Exception:  # noqa: BLE001
        # Fallback without Delta for local dev without delta jars
        return SparkSession.builder.appName(app_name).getOrCreate()
