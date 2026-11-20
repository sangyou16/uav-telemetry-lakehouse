"""Checks YOUR query in sql/gold/loss.sql. Skipped until you write it."""

import pytest

from uav_lakehouse.gold.build import LOSS_SQL, build_gold, stream_loss
from uav_lakehouse.sql_runner import load_sql

try:
    load_sql(LOSS_SQL)
except NotImplementedError:
    pytest.skip("write your query in sql/gold/loss.sql first", allow_module_level=True)


@pytest.fixture(scope="module")
def loss(spark, small_lake):
    p = small_lake["paths"]
    if p["gaps"] is None:
        pytest.skip("sql/silver/gaps.sql must be written first")
    rows = stream_loss(spark, p["telemetry"], p["gaps"]).collect()
    return {(r.aircraft_id, r.msg_type): r for r in rows}


def test_output_columns(loss):
    row = next(iter(loss.values()))
    assert set(row.asDict()) == {"aircraft_id", "msg_type", "received", "missing", "loss_pct"}


def test_one_row_per_stream(loss, small_lake):
    assert set(loss) == set(small_lake["streams"])


def test_stream_without_gaps_gets_zero_not_null(spark, small_lake):
    """Hide one stream's gaps: it must still appear, with missing = 0 and loss_pct = 0."""
    p = small_lake["paths"]
    if p["gaps"] is None:
        pytest.skip("sql/silver/gaps.sql must be written first")
    spark.read.format("delta").load(p["telemetry"]).createOrReplaceTempView("telemetry")
    spark.read.format("delta").load(p["gaps"]).filter(
        "NOT (aircraft_id = 'UAV-001' AND msg_type = 'battery')"
    ).createOrReplaceTempView("gaps")

    rows = (
        spark.sql(load_sql(LOSS_SQL))
        .filter("aircraft_id = 'UAV-001' AND msg_type = 'battery'")
        .collect()
    )

    assert len(rows) == 1, "a stream with no gaps disappeared: check your JOIN type"
    assert rows[0].missing == 0, "missing is NULL or wrong: NULL must become 0"
    assert float(rows[0].loss_pct) == 0.0


def test_missing_and_received_match_ground_truth(loss, small_lake):
    for key, s in small_lake["streams"].items():
        assert loss[key].missing == s["detectable_missing"], key  # 0, never NULL
        assert loss[key].received == s["sent"] - s["missing_seqs"], key


def test_loss_pct_formula(loss):
    for key, r in loss.items():
        expected = round(100 * r.missing / (r.received + r.missing), 3)
        assert float(r.loss_pct) == pytest.approx(expected, abs=1e-9), key


def test_build_gold_writes_one_row_per_stream(spark, small_lake, tmp_path):
    p = small_lake["paths"]
    n = build_gold(spark, p["telemetry"], p["gaps"], str(tmp_path / "health"))
    assert n == len(small_lake["streams"])
    df = spark.read.format("delta").load(str(tmp_path / "health"))
    assert df.filter("missing IS NULL OR loss_pct IS NULL").count() == 0
