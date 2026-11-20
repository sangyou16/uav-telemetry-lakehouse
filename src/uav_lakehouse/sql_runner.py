"""Run the hand-written .sql files in sql/ against Delta tables."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from uav_lakehouse.delta_utils import read_delta

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def load_sql(path: Path) -> str:
    """Return the query, ignoring comment lines. Raises if none has been written yet."""
    lines = [ln for ln in path.read_text().splitlines() if not ln.strip().startswith("--")]
    query = "\n".join(lines).strip().rstrip(";")
    if not query:
        raise NotImplementedError(f"no query written yet in {path}")
    return query


def run_sql_file(spark: SparkSession, path: Path, views: dict[str, str]) -> DataFrame:
    """Register each Delta table as a temp view, then run the query in `path`.

    views: {"view_name_used_in_sql": "/path/to/delta/table" or "catalog.schema.table"}
    """
    for name, table_path in views.items():
        read_delta(spark, table_path).createOrReplaceTempView(name)
    return spark.sql(load_sql(path))
