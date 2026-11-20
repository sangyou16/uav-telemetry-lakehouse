"""Bronze -> silver: parse, quarantine bad lines, dedupe, MERGE into Delta.

Two output tables:
  silver/telemetry   one row per unique valid message, typed and flattened
  silver/quarantine  every rejected line with the reason, so nothing disappears silently
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from uav_lakehouse.delta_utils import merge_insert_new

from .schema import MESSAGE_KEY
from .transform import dedupe_first_arrival, parse, rejected_lines, valid_messages


def build_silver(
    spark: SparkSession, bronze_path: str, telemetry_path: str, quarantine_path: str
) -> dict[str, int]:
    bronze = spark.read.format("delta").load(bronze_path)
    parsed = parse(bronze).cache()  # used twice below: parse once, not twice

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
        parsed.unpersist()
