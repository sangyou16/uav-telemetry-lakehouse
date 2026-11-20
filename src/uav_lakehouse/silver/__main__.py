"""CLI: python -m uav_lakehouse.silver --lake data/lake"""

from __future__ import annotations

import argparse
from pathlib import Path

from uav_lakehouse.spark import get_spark

from .build import build_silver
from .gaps import build_gaps


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Build silver tables from bronze.")
    p.add_argument("--lake", type=Path, default=Path("data/lake"))
    args = p.parse_args(argv)

    lake = args.lake.resolve()
    paths = {
        "bronze_path": str(lake / "bronze" / "telemetry_raw"),
        "telemetry_path": str(lake / "silver" / "telemetry"),
        "quarantine_path": str(lake / "silver" / "quarantine"),
    }
    gaps_path = str(lake / "silver" / "gaps")
    spark = get_spark("silver")
    try:
        counts = build_silver(spark, **paths)
        for name in ("telemetry", "quarantine"):
            total = spark.read.format("delta").load(paths[f"{name}_path"]).count()
            print(f"{name:<11} inserted {counts[f'{name}_inserted']:>7,}   total {total:>7,}")
        spark.read.format("delta").load(paths["quarantine_path"]).groupBy(
            "reject_reason"
        ).count().show()

        try:
            n_gaps = build_gaps(spark, paths["telemetry_path"], gaps_path)
            print(f"gaps        {n_gaps:,} rows (from sql/silver/gaps.sql)")
        except NotImplementedError:
            print("gaps        skipped: write your query in sql/silver/gaps.sql")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
