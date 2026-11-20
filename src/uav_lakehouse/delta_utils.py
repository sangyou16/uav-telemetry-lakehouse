"""Small Delta Lake helpers shared by the layers."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def merge_insert_new(spark: SparkSession, df: DataFrame, path: str, keys: tuple[str, ...]) -> int:
    """Insert rows whose key is not in the table yet. Returns rows inserted.

    MERGE ... WHEN NOT MATCHED THEN INSERT makes the write idempotent:
    running it again with the same input inserts nothing.
    The source must already be unique on `keys`, or both copies would be inserted.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").save(path)
        return spark.read.format("delta").load(path).count()

    table = DeltaTable.forPath(spark, path)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    table.alias("t").merge(df.alias("s"), on).whenNotMatchedInsertAll().execute()

    metrics = table.history(1).select("operationMetrics").first()["operationMetrics"]
    return int(metrics.get("numTargetRowsInserted", 0))
