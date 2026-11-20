import json

import pytest

from uav_lakehouse.bronze.ingest import ingest_landing
from uav_lakehouse.generator.config import SimConfig
from uav_lakehouse.generator.simulate import simulate
from uav_lakehouse.generator.writer import write_run

CFG = SimConfig(n_aircraft=2, duration_s=120, seed=3)


@pytest.fixture
def lake(tmp_path):
    """Fresh landing folder + bronze table paths for each test."""
    paths = {
        "landing": tmp_path / "landing",
        "bronze": str(tmp_path / "lake" / "bronze"),
        "checkpoint": str(tmp_path / "lake" / "_checkpoints" / "bronze"),
    }
    write_run(simulate(CFG), CFG, paths["landing"], "run-a")
    return paths


def ingest(spark, lake):
    return ingest_landing(spark, str(lake["landing"]), lake["bronze"], lake["checkpoint"])


def landing_lines(run_dir):
    return [ln for p in sorted(run_dir.glob("batch-*.jsonl")) for ln in p.read_text().splitlines()]


def test_every_line_lands_unchanged_including_corrupt(spark, lake):
    ingest(spark, lake)
    expected = landing_lines(lake["landing"] / "run-a")
    manifest = json.loads((lake["landing"] / "run-a" / "_manifest.json").read_text())
    assert manifest["totals"]["corrupted_lines"] > 0  # the test is meaningless otherwise

    rows = spark.read.format("delta").load(lake["bronze"]).select("raw_line").collect()
    assert sorted(r.raw_line for r in rows) == sorted(expected)


def test_manifest_is_not_ingested(spark, lake):
    ingest(spark, lake)
    files = spark.read.format("delta").load(lake["bronze"]).select("_source_file").distinct()
    names = [r._source_file.rsplit("/", 1)[-1] for r in files.collect()]
    assert names and all(n.startswith("batch-") and n.endswith(".jsonl") for n in names)


def test_metadata_columns_are_filled(spark, lake):
    ingest(spark, lake)
    df = spark.read.format("delta").load(lake["bronze"])
    for c in ("_source_file", "_source_file_mtime", "_ingest_ts"):
        assert df.filter(df[c].isNull()).count() == 0, c


def test_rerun_is_idempotent(spark, lake):
    first = ingest(spark, lake)
    second = ingest(spark, lake)
    assert first > 0 and second == 0
    assert spark.read.format("delta").load(lake["bronze"]).count() == first


def test_new_files_are_picked_up_incrementally(spark, lake):
    first = ingest(spark, lake)
    write_run(simulate(CFG), CFG, lake["landing"], "run-b")
    second = ingest(spark, lake)
    assert second == len(landing_lines(lake["landing"] / "run-b"))
    assert spark.read.format("delta").load(lake["bronze"]).count() == first + second
