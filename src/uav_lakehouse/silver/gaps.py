"""Run the hand-written gap-detection SQL against the silver telemetry table."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

GAPS_SQL = Path(__file__).resolve().parents[3] / "sql" / "silver" / "gaps.sql"


def load_sql(path: Path = GAPS_SQL) -> str:
    """Return the query, ignoring comment lines. Raises if none has been written yet."""
    lines = [ln for ln in path.read_text().splitlines() if not ln.strip().startswith("--")]
    query = "\n".join(lines).strip().rstrip(";")
    if not query:
        raise NotImplementedError(f"no query written yet in {path}")
    return query


def detect_gaps(spark: SparkSession, telemetry_path: str, sql_path: Path = GAPS_SQL) -> DataFrame:
    spark.read.format("delta").load(telemetry_path).createOrReplaceTempView("telemetry")
    return spark.sql(load_sql(sql_path))
