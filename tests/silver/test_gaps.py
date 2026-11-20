"""Checks YOUR query in sql/silver/gaps.sql. Skipped until you write it."""

import json

import pytest

from uav_lakehouse.bronze.ingest import ingest_landing
from uav_lakehouse.generator.config import SimConfig
from uav_lakehouse.generator.simulate import simulate
from uav_lakehouse.generator.writer import write_run
from uav_lakehouse.silver.build import build_silver
from uav_lakehouse.silver.gaps import detect_gaps, load_sql

CFG = SimConfig(n_aircraft=2, duration_s=120, seed=3)

try:
    load_sql()
except NotImplementedError:
    pytest.skip("write your query in sql/silver/gaps.sql first", allow_module_level=True)


@pytest.fixture(scope="module")
def gaps_and_truth(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("gaps")
    run_dir = write_run(simulate(CFG), CFG, root / "landing", "run-a")
    bronze, telemetry = str(root / "bronze"), str(root / "telemetry")
    ingest_landing(spark, str(root / "landing"), bronze, str(root / "ckpt"))
    build_silver(spark, bronze, telemetry, str(root / "quarantine"))
    manifest = json.loads((run_dir / "_manifest.json").read_text())
    return detect_gaps(spark, telemetry).collect(), manifest


def test_output_columns(gaps_and_truth):
    rows, _ = gaps_and_truth
    assert rows, "no gaps found, but the generator injected some"
    assert set(rows[0].asDict()) == {
        "aircraft_id",
        "msg_type",
        "prev_seq",
        "next_seq",
        "missing_count",
        "gap_start_ts",
        "gap_end_ts",
    }


def test_every_row_is_a_real_gap(gaps_and_truth):
    rows, _ = gaps_and_truth
    for r in rows:
        assert r.missing_count == r.next_seq - r.prev_seq - 1 >= 1, r
        assert r.gap_start_ts < r.gap_end_ts, r


def test_total_missing_matches_ground_truth(gaps_and_truth):
    rows, manifest = gaps_and_truth
    assert sum(r.missing_count for r in rows) == manifest["totals"]["detectable_missing"]


def test_missing_per_stream_matches_ground_truth(gaps_and_truth):
    rows, manifest = gaps_and_truth
    found = {}
    for r in rows:
        key = (r.aircraft_id, r.msg_type)
        found[key] = found.get(key, 0) + r.missing_count
    expected = {
        (s["aircraft_id"], s["msg_type"]): s["detectable_missing"]
        for s in manifest["streams"]
        if s["detectable_missing"] > 0
    }
    assert found == expected
