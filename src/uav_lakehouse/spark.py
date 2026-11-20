"""One place that builds a local SparkSession with Delta Lake enabled."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Must match the Hadoop version bundled inside pyspark (see tests/test_spark_config.py).
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.5.0"


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
        # Show and compute timestamps in UTC no matter where the laptop is.
        .config("spark.sql.session.timeZone", "UTC")
    )

    extra_packages = []
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    if endpoint:
        # S3 storage through Hadoop's s3a connector. Credentials are NOT set here:
        # s3a reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from the environment.
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config(
                "spark.hadoop.fs.s3a.endpoint.region",
                os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            )
            # Local S3 servers serve http://host/bucket/key, not http://bucket.host/key.
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
        )
        extra_packages.append(HADOOP_AWS_PACKAGE)  # ~650 MB AWS SDK, downloaded once

    # Downloads the matching jars (first run only, then cached in ~/.ivy2*).
    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
