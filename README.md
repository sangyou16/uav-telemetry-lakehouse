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
| 2 | Bronze: raw ingest into Delta | next |
| 3 | Silver: parse, dedupe, schema, gap detection | |
| 4 | Gold + SQL: rates, loss %, latency | |
| 5 | CI | lint + tests running |
| 6 | S3-compatible storage (MinIO), then Databricks Free Edition | |

## Quick start

```bash
pip install -e ".[dev]"
python -m uav_lakehouse.generator --aircraft 5 --duration 600 --seed 42
pytest
```

Default run: ~94k lines in `data/landing/run-42/`, written in arrival order as
one JSON Lines file per minute, plus `_manifest.json` with the ground truth
(sent, dropped, duplicated, late, buffered, corrupted, detectable gaps per stream).
