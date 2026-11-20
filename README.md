# uav-telemetry-lakehouse

Synthetic UAV telemetry through a bronze / silver / gold Delta Lake pipeline
(PySpark + Delta Lake). Runs entirely on free tools: local Spark, MinIO
(S3-compatible object storage) and Databricks Free Edition.

> **All data is synthetic.** Message types, fields and values are invented for
> this repo and do not mirror any real protocol, interface document or flight data.

Design, data model and fault model: [docs/DESIGN.md](docs/DESIGN.md)

## Status

| step | what | status |
|------|------|--------|
| 1 | Generator: N aircraft, 4 message types, injected faults, ground-truth manifest | done |
| 2 | Bronze: raw lines into Delta, incremental + idempotent (Structured Streaming `availableNow`) | done |
| 3 | Silver: parse, quarantine, dedupe, MERGE into Delta, sequence-gap detection in SQL | done |
| 4 | Gold: per-stream received rate, loss %, latency p50/p95/p99 (loss %: SQL exercise in `sql/gold/loss.sql`) | in progress |
| 5 | CI | lint + tests running |
| 6 | S3-compatible storage (MinIO), then Databricks Free Edition | |

## Quick start

Needs Python 3.11+ and Java 17+ (Spark 4). On Windows, use WSL Ubuntu.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,spark]"
python -m uav_lakehouse.generator --aircraft 5 --duration 600 --seed 42
python -m uav_lakehouse.bronze      # run it twice: the second run appends 0 rows
python -m uav_lakehouse.silver
python -m uav_lakehouse.gold
pytest
```

Default run: ~94k lines in `data/landing/run-42/`, written in arrival order as
one JSON Lines file per minute, plus `_manifest.json` with the ground truth
(sent, dropped, duplicated, late, buffered, corrupted, detectable gaps per stream).

Bronze then holds every raw line (94,273 rows, corrupt ones included) in
`data/lake/bronze/telemetry_raw`, with `_source_file`, `_source_file_mtime` and
`_ingest_ts` columns. A checkpoint in `data/lake/_checkpoints/bronze` records which
files are done, so re-runs never duplicate rows.

Silver turns those into `silver/telemetry` (93,271 unique, typed messages: one
per key, first arrival wins) and `silver/quarantine` (the 101 corrupt lines, each
with a reason). Both numbers equal the generator's ground truth.

Gold builds `gold/stream_health`: one row per (aircraft, message type) with
received count, missing count, loss %, received rate and latency percentiles.
The SQL behind it lives in `sql/gold/`.
