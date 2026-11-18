"""CLI: python -m uav_lakehouse.bronze --landing data/landing --lake data/lake"""

from __future__ import annotations

import argparse
from pathlib import Path

from uav_lakehouse.spark import get_spark

from .ingest import ingest_landing


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Ingest landing files into bronze Delta.")
    p.add_argument("--landing", type=Path, default=Path("data/landing"))
    p.add_argument("--lake", type=Path, default=Path("data/lake"))
    args = p.parse_args(argv)

    lake = args.lake.resolve()
    bronze_path = str(lake / "bronze" / "telemetry_raw")
    spark = get_spark("bronze")
    try:
        n = ingest_landing(
            spark,
            landing_dir=str(args.landing.resolve()),
            bronze_path=bronze_path,
            checkpoint_path=str(lake / "_checkpoints" / "bronze"),
        )
        total = spark.read.format("delta").load(bronze_path).count()
        print(f"appended {n:,} rows; bronze now has {total:,} rows at {bronze_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
