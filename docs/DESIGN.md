# Design: repo layout and data model

> All data in this project is **synthetic**. Message types, field names and
> values are invented for this repo and do not mirror any real interface
> control document, vendor protocol or flight-test data.

## Repo layout

```
uav-telemetry-lakehouse/
├── src/uav_lakehouse/
│   ├── generator/        # step 1: synthetic telemetry -> JSON Lines landing files
│   │   ├── config.py     #   dataclasses: message rates, fault knobs, sim settings
│   │   ├── fleet.py      #   aircraft + flight model -> message payloads
│   │   ├── simulate.py   #   clean streams + fault injection + ground-truth counts
│   │   ├── writer.py     #   arrival-ordered batch files + _manifest.json
│   │   └── __main__.py   #   CLI: python -m uav_lakehouse.generator
│   ├── bronze/           # step 2: raw lines -> Delta (append + ingestion metadata)
│   ├── silver/           # step 3: parse, dedupe, schema, sequence-gap detection
│   └── gold/             # step 4: per-aircraft rates, loss %, latency
├── sql/                  # step 4: SQL on top of gold (written by hand)
├── tests/                # pytest, one folder per layer
├── scripts/              # step 6: run_on_s3.sh (local S3-compatible server)
├── docs/                 # this file, architecture diagram
└── .github/workflows/    # CI: lint + tests
```

## The data flow

```
generator ──► landing/*.jsonl ──► bronze (Delta) ──► silver (Delta) ──► gold (Delta) ──► SQL
              arrival order       raw string +        typed, deduped,     rates, loss %,
              + _manifest.json    ingest metadata     gap table           latency
```

The manifest holds the **ground truth** (what was really sent, dropped,
duplicated). Silver and gold are tested against it: if gold says 1.2 % loss
for an aircraft, the manifest says whether that is right.

## Landing record (one JSON object per line)

The aircraft stamps `event_ts_ms` and `seq`. The ground receiver stamps
`recv_ts_ms` on arrival. Files are written in **arrival order**, the way a
receiver would write them, so late messages appear out of order.

| field            | type   | set by   | notes                                         |
|------------------|--------|----------|-----------------------------------------------|
| `schema_version` | int    | aircraft | always `1` for now (room for schema evolution)|
| `aircraft_id`    | string | aircraft | `UAV-001` … `UAV-NNN`                         |
| `msg_type`       | string | aircraft | see table below                               |
| `seq`            | int    | aircraft | 0,1,2,… per `(aircraft_id, msg_type)` stream  |
| `event_ts_ms`    | int    | aircraft | epoch ms when the message was produced        |
| `recv_ts_ms`     | int    | ground   | epoch ms when the ground received it          |
| `payload`        | object | aircraft | shape depends on `msg_type`                   |

**Key:** `(aircraft_id, msg_type, seq)` identifies one message. Duplicates share
the key and differ only in `recv_ts_ms`.
**Latency:** `recv_ts_ms - event_ts_ms`.
**Loss:** a missing `seq` inside a stream.

### Message types (invented)

| `msg_type`  | default rate | payload fields                                              |
|-------------|-------------:|-------------------------------------------------------------|
| `heartbeat` | 1 Hz         | `mode`, `armed`, `health`                                   |
| `position`  | 10 Hz        | `lat_deg`, `lon_deg`, `alt_m`, `ground_speed_mps`, `heading_deg` |
| `attitude`  | 20 Hz        | `roll_deg`, `pitch_deg`, `yaw_deg`                          |
| `battery`   | 0.5 Hz       | `voltage_v`, `current_a`, `remaining_pct`                   |

Aircraft fly circles around invented points near 0°N 30°W (open ocean), so no
coordinate means anything real.

## Faults the generator injects

| fault              | what happens                                              | who should catch it |
|--------------------|-----------------------------------------------------------|---------------------|
| random drop        | single message never arrives                              | silver gap detection |
| link outage        | every message from one aircraft lost for a few seconds    | silver gap detection (long gap) |
| store-and-forward  | messages held for a few seconds, then arrive all at once  | gold latency + rate burst |
| late message       | extra delay, arrives after newer messages                 | silver ordering by `seq`/`event_ts` |
| duplicate          | same message arrives twice                                | silver dedupe |
| corrupt line       | line is truncated, not valid JSON                         | silver quarantine |

## Ground-truth manifest (`_manifest.json`)

Per stream `(aircraft_id, msg_type)`:

- `sent`, `dropped_random`, `dropped_outage`, `duplicates`, `late`, `buffered`, `corrupted_lines`
- `missing_seqs`: seqs with no valid copy delivered
- `detectable_missing`: missing seqs between the first and last valid seq.
  A gap detector only sees holes *between* two received messages, so losses at
  the very start or end of a stream are invisible to it. Silver's gap count
  should equal this number exactly.

## Bronze table (`bronze/telemetry_raw`)

| column               | type      | notes                                             |
|----------------------|-----------|---------------------------------------------------|
| `raw_line`           | string    | the line exactly as received, never parsed here   |
| `_source_file`       | string    | landing file it came from (lineage, replay)       |
| `_source_file_mtime` | timestamp | when that file was written                        |
| `_ingest_ts`         | timestamp | when bronze ingested it                           |

- **Why not parse JSON in bronze?** Corrupt lines must survive so silver can
  quarantine and count them. Bronze is the "we received exactly this" log.
- **Why streaming for a batch job?** `trigger(availableNow=True)` + checkpoint
  = process only new files, then stop. Re-runs are idempotent at the file level.
- **Why no partitioning?** ~100k rows. Partitioning tiny tables creates many
  tiny files and slows reads. Revisit at real volume (e.g. by ingest date).

## Silver tables

**`silver/telemetry`**: one row per message key `(aircraft_id, msg_type, seq)`.
Columns: the key, `event_ts`, `recv_ts` (UTC timestamps), `latency_ms`, the raw
epoch-ms values, every payload field flattened into its own typed column, and
`_source_file`, `_ingest_ts` for lineage.

**`silver/quarantine`**: every rejected line: `raw_line`, `reject_reason`,
`_source_file`, `_ingest_ts`. Rules, first match wins:

| reason                       | rule                                           |
|------------------------------|------------------------------------------------|
| `unparseable`                | not valid JSON (`_corrupt_record` is set)      |
| `unsupported_schema_version` | `schema_version` is not 1                      |
| `missing_required_field`     | key, `event_ts_ms` or `recv_ts_ms` is NULL     |
| `unknown_msg_type`           | not one of the four known types                |
| `invalid_seq`                | `seq < 0`                                      |

Design choices:

- **Explicit `_corrupt_record`** instead of "all fields are NULL": the check
  says what it means and doesn't depend on parser defaults.
- **One wide table, not one per msg_type.** Loss and latency are computed across
  all types with the same envelope columns. Unused payload columns are NULL,
  which Parquet stores almost for free.
- **Dedupe = first arrival wins**: `ROW_NUMBER() OVER (PARTITION BY key ORDER BY
  recv_ts_ms, ...) = 1`, with extra ORDER BY columns so ties are deterministic.
- **MERGE insert-only on the key** (`WHEN NOT MATCHED THEN INSERT`): re-runs insert
  nothing, and a duplicate that arrives in a later bronze batch is not inserted
  again. The source must be deduped first, or MERGE would insert both copies.
- **Full read of bronze each run.** Simple and fine at this size. At scale, read
  bronze incrementally (streaming + `foreachBatch` running the same MERGE).
- **Gap detection is SQL** (`sql/silver/gaps.sql`), `LAG(seq)` per stream.
  It can only see holes between two received messages, so it should find exactly
  the manifest's `detectable_missing`.

## Gold table (`gold/stream_health`)

One row per stream `(aircraft_id, msg_type)`:

| column | meaning |
|---|---|
| `received` | unique messages in silver |
| `missing` | sum of `missing_count` from `silver/gaps` (0 if none) |
| `loss_pct` | `100 * missing / (received + missing)` |
| `received_rate_hz` | `(received - 1) / span_seconds`: N messages span N-1 intervals |
| `latency_p50_ms` … `latency_p99_ms`, `latency_max_ms` | `recv_ts - event_ts` percentiles |
| `first_event_ts`, `last_event_ts` | time span of the stream |

Design choices:

- **Metrics in SQL files** (`sql/gold/stream_stats.sql`, `sql/gold/loss.sql`);
  Python only maps tables to views, joins and writes.
- **Overwrite, not MERGE.** Gold is a small aggregate over all of silver, so a
  full recompute is cheap and can never drift. `silver/gaps` is also recomputed:
  a late message can close or split an old gap, which would make MERGEd gap rows wrong.
- **Aggregate before joining.** Joining `telemetry` (thousands of rows per stream)
  to `gaps` (a few rows per stream) before `GROUP BY` multiplies rows and breaks
  `COUNT(*)` and `SUM`.
- **LEFT JOIN + COALESCE.** A stream with no gaps has no rows in `gaps`; it must
  still appear, with `missing = 0`, not NULL and not dropped.
- **Percentiles, not averages.** A store-and-forward burst barely moves the mean
  latency but dominates p99, which is what an operator cares about.

## Running on S3 (step 6a)

- **Storage-agnostic layers.** Every layer takes table locations as strings;
  `paths.lake_tables()` builds them under a local folder or an `s3a://` root.
  No layer code changed to move the lake to S3.
- **Opt-in S3 support.** `get_spark()` adds the s3a connector only when
  `S3_ENDPOINT_URL` is set, because its AWS SDK is a ~650 MB download.
- **`hadoop-aws` version = Hadoop inside pyspark** (3.5.0 for pyspark 4.2). A
  mismatch fails at runtime with class errors; a unit test guards it.
- **Credentials from the environment** (`AWS_ACCESS_KEY_ID`, ...), never in code
  or config files; s3a's default credential chain reads them.
- **Path-style access** (`http://host/bucket/key`), which local S3 servers need.
- **Local server: moto**, a pip-installable S3-compatible server. MinIO was the
  original plan, but its free community edition stopped shipping binaries in
  late 2025 and is no longer maintained.
- **Checkpoints live in S3 too,** so bronze stays idempotent there (tested).

## Running on Databricks (step 6b)

- **Paths or table names, same code.** `delta_utils` reads, writes, MERGEs and
  streams to either a path or a catalog name (`workspace.uav.silver_telemetry`);
  a name has no `/`. `paths.catalog_tables()` gives the Unity Catalog names.
- **Volumes for files.** Landing files and the bronze streaming checkpoint live in
  `/Volumes/<catalog>/<schema>/raw`; tables are Unity Catalog managed tables.
- **Serverless = Spark Connect.** It has no `cache()`, so silver caches only when
  `pyspark.sql.utils.is_remote()` is false. Bronze counts new rows as
  "after − before" instead of reading streaming progress objects.
- **Tested without a workspace.** `tests/test_catalog_tables.py` runs the pipeline
  with three-part table names on local Spark; `tests/test_databricks_notebook.py`
  executes the notebook top to bottom with stand-ins for `dbutils` and `display`.
