"""One place that builds a local SparkSession with Delta Lake enabled."""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "uav-lakehouse") -> SparkSession:
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        # These two lines turn plain Spark into "Spark + Delta Lake".
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Small data on one machine: 200 shuffle partitions (the default) is overkill.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    # Downloads the matching Delta jars (first run only, then cached in ~/.ivy2).
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
