"""CLI: python -m uav_lakehouse.bronze --landing data/landing --lake data/lake

Both locations may be local folders or s3a:// URIs.
"""

from __future__ import annotations

import argparse

from uav_lakehouse.paths import lake_tables, normalize
from uav_lakehouse.spark import get_spark

from .ingest import ingest_landing


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Ingest landing files into bronze Delta.")
    p.add_argument("--landing", default="data/landing")
    p.add_argument("--lake", default="data/lake")
    args = p.parse_args(argv)

    t = lake_tables(args.lake)
    spark = get_spark("bronze")
    try:
        n = ingest_landing(spark, normalize(args.landing), t["bronze"], t["bronze_checkpoint"])
        total = spark.read.format("delta").load(t["bronze"]).count()
        print(f"appended {n:,} rows; bronze now has {total:,} rows at {t['bronze']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
