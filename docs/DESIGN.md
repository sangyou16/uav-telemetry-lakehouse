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
├── infra/                # step 6: MinIO (local S3-compatible storage), Databricks
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
