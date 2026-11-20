"""Small Delta Lake helpers shared by the layers.

Every table location can be either
  * a path:        /home/me/data/lake/silver/telemetry   or   s3a://lake/silver/telemetry
  * a table name:  workspace.uav.silver_telemetry        (Unity Catalog on Databricks)
A name never contains "/", a path always does. These helpers hide the difference,
so the layers run unchanged on a laptop, on S3 and on Databricks.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def is_table_name(location: str) -> bool:
    return "/" not in location


def read_delta(spark: SparkSession, location: str) -> DataFrame:
    if is_table_name(location):
        return spark.read.table(location)
    return spark.read.format("delta").load(location)


def delta_exists(spark: SparkSession, location: str) -> bool:
    if is_table_name(location):
        return spark.catalog.tableExists(location)
    return DeltaTable.isDeltaTable(spark, location)


def delta_table(spark: SparkSession, location: str) -> DeltaTable:
    if is_table_name(location):
        return DeltaTable.forName(spark, location)
    return DeltaTable.forPath(spark, location)


def write_delta(df: DataFrame, location: str, mode: str) -> None:
    """mode: "append" or "overwrite" (overwrite also replaces the schema)."""
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if is_table_name(location):
        writer.saveAsTable(location)
    else:
        writer.save(location)


def overwrite_delta(spark: SparkSession, df: DataFrame, location: str) -> int:
    """Replace the table's contents with df. Returns the new row count."""
    write_delta(df, location, "overwrite")
    return read_delta(spark, location).count()


def merge_insert_new(
    spark: SparkSession, df: DataFrame, location: str, keys: tuple[str, ...]
) -> int:
    """Insert rows whose key is not in the table yet. Returns rows inserted.

    MERGE ... WHEN NOT MATCHED THEN INSERT makes the write idempotent:
    running it again with the same input inserts nothing.
    The source must already be unique on `keys`, or both copies would be inserted.
    """
    if not delta_exists(spark, location):
        write_delta(df, location, "append")
        return read_delta(spark, location).count()

    table = delta_table(spark, location)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    table.alias("t").merge(df.alias("s"), on).whenNotMatchedInsertAll().execute()

    metrics = table.history(1).select("operationMetrics").first()["operationMetrics"]
    return int(metrics.get("numTargetRowsInserted", 0))
