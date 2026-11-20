"""Bronze -> silver: parse, quarantine bad lines, dedupe, MERGE into Delta.

Two output tables:
  silver/telemetry   one row per unique valid message, typed and flattened
  silver/quarantine  every rejected line with the reason, so nothing disappears silently
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.utils import is_remote

from uav_lakehouse.delta_utils import merge_insert_new, read_delta

from .schema import MESSAGE_KEY
from .transform import dedupe_first_arrival, parse, rejected_lines, valid_messages


def build_silver(
    spark: SparkSession, bronze_path: str, telemetry_path: str, quarantine_path: str
) -> dict[str, int]:
    parsed = parse(read_delta(spark, bronze_path))
    # Used twice below, so cache it: parse once, not twice. Serverless compute
    # (Spark Connect, e.g. Databricks Free Edition) has no cache API: skip it there.
    if not is_remote():
        parsed = parsed.cache()

    try:
        messages = dedupe_first_arrival(valid_messages(parsed))
        rejects = rejected_lines(parsed)

        return {
            "telemetry_inserted": merge_insert_new(spark, messages, telemetry_path, MESSAGE_KEY),
            "quarantine_inserted": merge_insert_new(
                spark, rejects, quarantine_path, ("_source_file", "raw_line")
            ),
        }
    finally:
        if not is_remote():
            parsed.unpersist()
