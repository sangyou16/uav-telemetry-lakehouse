"""Silver -> gold/stream_health: one row per (aircraft_id, msg_type).

The metrics live in SQL files; this module only wires tables to views,
joins the two results and writes Delta.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from uav_lakehouse.sql_runner import SQL_DIR, run_sql_file

STREAM_KEY = ["aircraft_id", "msg_type"]
STATS_SQL = SQL_DIR / "gold" / "stream_stats.sql"
LOSS_SQL = SQL_DIR / "gold" / "loss.sql"


def stream_stats(spark: SparkSession, telemetry_path: str) -> DataFrame:
    return run_sql_file(spark, STATS_SQL, {"telemetry": telemetry_path})


def stream_loss(spark: SparkSession, telemetry_path: str, gaps_path: str) -> DataFrame:
    return run_sql_file(spark, LOSS_SQL, {"telemetry": telemetry_path, "gaps": gaps_path})


def build_gold(spark: SparkSession, telemetry_path: str, gaps_path: str, health_path: str) -> int:
    """Recompute gold/stream_health and overwrite it. Returns the number of streams.

    Overwrite, not MERGE: gold is a small aggregate of all of silver, so
    recomputing it is cheap and can never drift out of sync.
    """
    stats = stream_stats(spark, telemetry_path)
    loss = stream_loss(spark, telemetry_path, gaps_path).select(*STREAM_KEY, "missing", "loss_pct")
    health = stats.join(loss, STREAM_KEY, "left").select(
        *STREAM_KEY,
        "received",
        "missing",
        F.col("loss_pct").cast("double").alias("loss_pct"),  # DECIMAL or DOUBLE in -> DOUBLE out
        "received_rate_hz",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "latency_max_ms",
        "first_event_ts",
        "last_event_ts",
    )
    health.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        health_path
    )
    return spark.read.format("delta").load(health_path).count()
