import json

import pytest

from uav_lakehouse.bronze.ingest import ingest_landing
from uav_lakehouse.generator.config import DEFAULT_MESSAGES, SimConfig
from uav_lakehouse.generator.simulate import simulate
from uav_lakehouse.generator.writer import write_run
from uav_lakehouse.silver.build import build_silver
from uav_lakehouse.silver.schema import KNOWN_MSG_TYPES

CFG = SimConfig(n_aircraft=2, duration_s=120, seed=3)


def make_paths(root):
    lake = root / "lake"
    return {
        "bronze_path": str(lake / "bronze"),
        "telemetry_path": str(lake / "silver" / "telemetry"),
        "quarantine_path": str(lake / "silver" / "quarantine"),
    }


@pytest.fixture(scope="module")
def built(spark, tmp_path_factory):
    """Generator -> landing -> bronze -> silver, once for the read-only tests below."""
    root = tmp_path_factory.mktemp("silver")
    result = simulate(CFG)
    run_dir = write_run(result, CFG, root / "landing", "run-a")
    paths = make_paths(root)
    ingest_landing(spark, str(root / "landing"), paths["bronze_path"], str(root / "ckpt"))
    counts = build_silver(spark, **paths)
    manifest = json.loads((run_dir / "_manifest.json").read_text())
    return {"result": result, "manifest": manifest, "paths": paths, "counts": counts}


def read(spark, built, table):
    return spark.read.format("delta").load(built["paths"][f"{table}_path"])


def test_contract_matches_generator():
    assert set(KNOWN_MSG_TYPES) == {m.msg_type for m in DEFAULT_MESSAGES}


def test_every_corrupt_line_is_quarantined(spark, built):
    q = read(spark, built, "quarantine")
    assert built["manifest"]["totals"]["corrupted_lines"] > 0
    assert q.count() == built["manifest"]["totals"]["corrupted_lines"]
    assert {r.reject_reason for r in q.select("reject_reason").distinct().collect()} == {
        "unparseable"
    }


def test_one_row_per_message_and_first_arrival_wins(spark, built):
    # Oracle in plain Python: for each key, the earliest valid (non-corrupt) copy.
    expected = {}
    for d in built["result"].deliveries:
        if not d.corrupt:
            key = (d.aircraft_id, d.msg_type, d.seq)
            expected[key] = min(expected.get(key, d.recv_ts_ms), d.recv_ts_ms)

    rows = (
        read(spark, built, "telemetry")
        .select("aircraft_id", "msg_type", "seq", "recv_ts_ms")
        .collect()
    )
    actual = {(r.aircraft_id, r.msg_type, r.seq): r.recv_ts_ms for r in rows}

    assert len(rows) == len(actual)  # no duplicate keys
    assert actual == expected


def test_row_count_matches_manifest(spark, built):
    t = built["manifest"]["totals"]
    assert read(spark, built, "telemetry").count() == t["sent"] - t["missing_seqs"]


def test_types_latency_and_flattened_payload(spark, built):
    df = read(spark, built, "telemetry")
    types = dict(df.dtypes)
    assert types["seq"] == "bigint" and types["event_ts"] == "timestamp"
    assert types["lat_deg"] == "double" and types["armed"] == "boolean"

    assert df.filter("latency_ms != recv_ts_ms - event_ts_ms OR latency_ms < 0").count() == 0
    # Each msg_type fills only its own payload columns.
    assert df.filter("msg_type = 'position' AND lat_deg IS NULL").count() == 0
    assert df.filter("msg_type = 'attitude' AND lat_deg IS NOT NULL").count() == 0
    assert df.filter("msg_type = 'heartbeat' AND mode IS NULL").count() == 0


def test_rerun_inserts_nothing(spark, built):
    before = read(spark, built, "telemetry").count()
    again = build_silver(spark, **built["paths"])
    assert again == {"telemetry_inserted": 0, "quarantine_inserted": 0}
    assert read(spark, built, "telemetry").count() == before


@pytest.mark.parametrize(
    ("line", "reason"),
    [
        ('{"schema_version":1,"aircraft_id":"UAV-0', "unparseable"),
        ("not json at all", "unparseable"),
        (
            '{"schema_version":2,"aircraft_id":"UAV-001","msg_type":"position",'
            '"seq":1,"event_ts_ms":1,"recv_ts_ms":2,"payload":{}}',
            "unsupported_schema_version",
        ),
        (
            '{"schema_version":1,"aircraft_id":"UAV-001","msg_type":"position",'
            '"event_ts_ms":1,"recv_ts_ms":2,"payload":{}}',
            "missing_required_field",
        ),
        (
            '{"schema_version":1,"aircraft_id":"UAV-001","msg_type":"weather",'
            '"seq":1,"event_ts_ms":1,"recv_ts_ms":2,"payload":{}}',
            "unknown_msg_type",
        ),
        (
            '{"schema_version":1,"aircraft_id":"UAV-001","msg_type":"position",'
            '"seq":-1,"event_ts_ms":1,"recv_ts_ms":2,"payload":{}}',
            "invalid_seq",
        ),
    ],
)
def test_reject_reasons(spark, tmp_path, line, reason):
    paths = make_paths(tmp_path)
    spark.sql("SELECT current_timestamp() AS _ingest_ts").selectExpr(
        f"{json.dumps(line)} AS raw_line", "'hand-made.jsonl' AS _source_file", "_ingest_ts"
    ).write.format("delta").save(paths["bronze_path"])

    counts = build_silver(spark, **paths)

    assert counts == {"telemetry_inserted": 0, "quarantine_inserted": 1}
    q = spark.read.format("delta").load(paths["quarantine_path"]).first()
    assert q.reject_reason == reason
