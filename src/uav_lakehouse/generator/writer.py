"""Write deliveries as arrival-ordered JSON Lines batches, plus a manifest.

Output layout:

    <out_dir>/<run_id>/batch-00000.jsonl   # one file per `file_seconds` of receive time
    <out_dir>/<run_id>/batch-00001.jsonl
    <out_dir>/<run_id>/_manifest.json      # config + ground truth (Spark skips "_" files)
"""

from __future__ import annotations

import json
from dataclasses import asdict
from itertools import groupby
from pathlib import Path

from .config import SimConfig
from .simulate import Delivery, SimResult


def serialize(d: Delivery) -> str:
    line = json.dumps(d.to_record(), separators=(",", ":"))
    if d.corrupt:
        line = line[: len(line) // 2]  # truncated mid-record: not valid JSON
    return line


def write_run(
    result: SimResult, cfg: SimConfig, out_dir: Path, run_id: str, file_seconds: int = 60
) -> Path:
    run_dir = Path(out_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # Arrival order, as the receiver would see it. Tie-breakers keep it deterministic.
    ordered = sorted(
        result.deliveries, key=lambda d: (d.recv_ts_ms, d.aircraft_id, d.msg_type, d.seq)
    )

    def batch_index(d: Delivery) -> int:
        return (d.recv_ts_ms - cfg.start_ts_ms) // (file_seconds * 1000)

    n_files = 0
    for idx, batch in groupby(ordered, key=batch_index):
        path = run_dir / f"batch-{idx:05d}.jsonl"
        path.write_text("".join(serialize(d) + "\n" for d in batch))
        n_files += 1

    streams = [
        {"aircraft_id": ac, "msg_type": mt, **stats.as_dict()}
        for (ac, mt), stats in sorted(result.stats.items())
    ]
    totals = (
        {k: sum(s[k] for s in streams) for k in streams[0] if k not in ("aircraft_id", "msg_type")}
        if streams
        else {}
    )

    manifest = {
        "run_id": run_id,
        "config": asdict(cfg),
        "files": n_files,
        "lines": len(ordered),
        "totals": totals,
        "streams": streams,
    }
    (run_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2))
    return run_dir
