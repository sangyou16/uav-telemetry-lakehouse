"""Run the hand-written gap-detection SQL and store the result as silver/gaps."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from uav_lakehouse.sql_runner import SQL_DIR, run_sql_file

GAPS_SQL = SQL_DIR / "silver" / "gaps.sql"


def detect_gaps(spark: SparkSession, telemetry_path: str, sql_path: Path = GAPS_SQL) -> DataFrame:
    return run_sql_file(spark, sql_path, {"telemetry": telemetry_path})


def build_gaps(spark: SparkSession, telemetry_path: str, gaps_path: str) -> int:
    """Recompute all gaps and overwrite the table. Returns the number of gaps.

    Overwrite, not MERGE: a late message can split or close an existing gap,
    so old gap rows may become wrong. Recomputing is simple and always correct.
    """
    gaps = detect_gaps(spark, telemetry_path)
    gaps.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gaps_path)
    return spark.read.format("delta").load(gaps_path).count()
