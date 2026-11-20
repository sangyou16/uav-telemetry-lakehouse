"""CLI: python -m uav_lakehouse.gold --lake data/lake"""

from __future__ import annotations

import argparse
from pathlib import Path

from uav_lakehouse.spark import get_spark

from .build import build_gold


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Build gold tables from silver.")
    p.add_argument("--lake", type=Path, default=Path("data/lake"))
    args = p.parse_args(argv)

    lake = args.lake.resolve()
    health_path = str(lake / "gold" / "stream_health")
    spark = get_spark("gold")
    try:
        n = build_gold(
            spark,
            telemetry_path=str(lake / "silver" / "telemetry"),
            gaps_path=str(lake / "silver" / "gaps"),
            health_path=health_path,
        )
        print(f"gold/stream_health: {n} streams")
        spark.read.format("delta").load(health_path).drop(
            "first_event_ts", "last_event_ts"
        ).orderBy("aircraft_id", "msg_type").show(n, truncate=False)
    except NotImplementedError as e:
        print(f"gold skipped: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
