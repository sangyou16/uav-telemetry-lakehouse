"""Pure DataFrame -> DataFrame steps. No I/O here, so each one is easy to test."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .schema import (
    KNOWN_MSG_TYPES,
    MESSAGE_KEY,
    RAW_DDL,
    REQUIRED_FIELDS,
    SUPPORTED_SCHEMA_VERSION,
)


def parse(bronze: DataFrame) -> DataFrame:
    """Parse raw_line and attach a reject_reason (NULL means the row is valid)."""
    j = bronze.withColumn(
        "j",
        F.from_json("raw_line", RAW_DDL, {"columnNameOfCorruptRecord": "_corrupt_record"}),
    )

    missing_required = F.lit(False)
    for name in REQUIRED_FIELDS:
        missing_required = missing_required | F.col(f"j.{name}").isNull()

    # First matching rule wins, so order matters: check "is it JSON" before anything else.
    reject_reason = (
        F.when(F.col("j._corrupt_record").isNotNull(), "unparseable")
        .when(
            F.coalesce(F.col("j.schema_version") != SUPPORTED_SCHEMA_VERSION, F.lit(True)),
            "unsupported_schema_version",
        )
        .when(missing_required, "missing_required_field")
        .when(~F.col("j.msg_type").isin(*KNOWN_MSG_TYPES), "unknown_msg_type")
        .when(F.col("j.seq") < 0, "invalid_seq")
    )
    return j.withColumn("reject_reason", reject_reason)


def valid_messages(parsed: DataFrame) -> DataFrame:
    """Typed, flattened rows for messages that passed every rule."""
    return parsed.filter(F.col("reject_reason").isNull()).select(
        "j.aircraft_id",
        "j.msg_type",
        "j.seq",
        F.timestamp_millis("j.event_ts_ms").alias("event_ts"),
        F.timestamp_millis("j.recv_ts_ms").alias("recv_ts"),
        (F.col("j.recv_ts_ms") - F.col("j.event_ts_ms")).alias("latency_ms"),
        "j.event_ts_ms",
        "j.recv_ts_ms",
        "j.payload.*",  # flatten: one column per payload field
        "_source_file",
        "_ingest_ts",
    )


def rejected_lines(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("reject_reason").isNotNull()).select(
        "raw_line", "reject_reason", "_source_file", "_ingest_ts"
    )


def dedupe_first_arrival(messages: DataFrame) -> DataFrame:
    """Keep one row per message key: the copy that arrived first."""
    first_arrival = Window.partitionBy(*MESSAGE_KEY).orderBy(
        "recv_ts_ms",
        "_ingest_ts",
        "_source_file",  # extra columns break exact ties
    )
    return (
        messages.withColumn("_rn", F.row_number().over(first_arrival))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
