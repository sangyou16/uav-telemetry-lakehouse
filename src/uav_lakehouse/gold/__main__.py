"""CLI: python -m uav_lakehouse.gold --lake data/lake   (local folder or s3a:// URI)"""

from __future__ import annotations

import argparse

from uav_lakehouse.paths import lake_tables
from uav_lakehouse.spark import get_spark

from .build import build_gold


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Build gold tables from silver.")
    p.add_argument("--lake", default="data/lake")
    args = p.parse_args(argv)

    t = lake_tables(args.lake)
    spark = get_spark("gold")
    try:
        n = build_gold(spark, t["telemetry"], t["gaps"], t["stream_health"])
        print(f"gold/stream_health: {n} streams")
        spark.read.format("delta").load(t["stream_health"]).drop(
            "first_event_ts", "last_event_ts"
        ).orderBy("aircraft_id", "msg_type").show(n, truncate=False)
    except NotImplementedError as e:
        print(f"gold skipped: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
