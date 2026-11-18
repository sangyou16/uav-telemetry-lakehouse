"""CLI: python -m uav_lakehouse.generator --aircraft 5 --duration 600"""

from __future__ import annotations

import argparse
from pathlib import Path

from .config import FaultConfig, SimConfig
from .simulate import simulate
from .writer import write_run


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Generate synthetic UAV telemetry.")
    p.add_argument("--aircraft", type=int, default=5)
    p.add_argument("--duration", type=float, default=600.0, help="seconds of flight")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", type=Path, default=Path("data/landing"))
    p.add_argument("--run-id", default=None, help="default: run-<seed>")
    p.add_argument("--file-seconds", type=int, default=60, help="receive-time span per file")
    p.add_argument("--no-faults", action="store_true", help="perfect link")
    args = p.parse_args(argv)

    cfg = SimConfig(
        n_aircraft=args.aircraft,
        duration_s=args.duration,
        seed=args.seed,
        faults=FaultConfig.none() if args.no_faults else FaultConfig(),
    )
    run_id = args.run_id or f"run-{args.seed}"
    result = simulate(cfg)
    run_dir = write_run(result, cfg, args.out, run_id, args.file_seconds)

    t = {
        k: sum(getattr(s, k) for s in result.stats.values())
        for k in (
            "sent",
            "dropped_random",
            "dropped_outage",
            "duplicates",
            "late",
            "buffered",
            "corrupted_lines",
            "detectable_missing",
        )
    }
    print(f"wrote {len(result.deliveries):,} lines to {run_dir}")
    for k, v in t.items():
        print(f"  {k:<20}{v:>10,}")


if __name__ == "__main__":
    main()
