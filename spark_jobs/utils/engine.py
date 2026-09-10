"""Spark session factory for FreightLake."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "FreightLake") -> SparkSession:
    # Packages needed for local runs: postgres jdbc and mongo connector via SPARK_PACKAGES.
    # Delta is provided via delta-spark pip package using configure_spark_with_delta_pip,
    # which selects the correct Delta version for the installed pyspark (4.2 + Scala 2.13).
    # On Databricks, libs are preinstalled, so we skip pip helper.
    is_databricks = bool(os.getenv("DATABRICKS_RUNTIME_VERSION"))
    try:
        builder = SparkSession.builder.appName(app_name)
        if not is_databricks:
            try:
                from delta import configure_spark_with_delta_pip

                builder = configure_spark_with_delta_pip(builder)
            except ImportError:
                builder = (
                    builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config(
                        "spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    )
                )
            # Postgres and Mongo jars for bronze JDBC/Mongo connector
            pg_mongo = os.getenv(
                "SPARK_PACKAGES",
                "org.postgresql:postgresql:42.7.3,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1",
            )
            builder = builder.config("spark.jars.packages", pg_mongo)
        else:
            builder = (
                builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
            )
        builder = builder.config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        return builder.getOrCreate()
    except Exception:  # noqa: BLE001
        return SparkSession.builder.appName(app_name).getOrCreate()
