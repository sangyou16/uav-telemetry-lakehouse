"""CLI: python -m uav_lakehouse.silver --lake data/lake   (local folder or s3a:// URI)"""

from __future__ import annotations

import argparse

from uav_lakehouse.paths import lake_tables
from uav_lakehouse.spark import get_spark

from .build import build_silver
from .gaps import build_gaps


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Build silver tables from bronze.")
    p.add_argument("--lake", default="data/lake")
    args = p.parse_args(argv)

    t = lake_tables(args.lake)
    spark = get_spark("silver")
    try:
        counts = build_silver(spark, t["bronze"], t["telemetry"], t["quarantine"])
        for name in ("telemetry", "quarantine"):
            total = spark.read.format("delta").load(t[name]).count()
            print(f"{name:<11} inserted {counts[f'{name}_inserted']:>7,}   total {total:>7,}")
        spark.read.format("delta").load(t["quarantine"]).groupBy("reject_reason").count().show()

        try:
            n_gaps = build_gaps(spark, t["telemetry"], t["gaps"])
            print(f"gaps        {n_gaps:,} rows (from sql/silver/gaps.sql)")
        except NotImplementedError:
            print("gaps        skipped: write your query in sql/silver/gaps.sql")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
