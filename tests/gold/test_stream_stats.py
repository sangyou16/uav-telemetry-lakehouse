import math
from collections import defaultdict
from dataclasses import replace

import pytest

from uav_lakehouse.generator.config import FaultConfig
from uav_lakehouse.gold.build import stream_stats


def exact_percentile(values, p):
    """Nearest-rank percentile: the smallest value with at least p of the data <= it."""
    ordered = sorted(values)
    return ordered[math.ceil(p * len(ordered)) - 1]


def stats_by_stream(spark, telemetry_path):
    rows = stream_stats(spark, telemetry_path).collect()
    return {(r.aircraft_id, r.msg_type): r for r in rows}


def test_one_row_per_stream_with_correct_count(spark, small_lake):
    stats = stats_by_stream(spark, small_lake["paths"]["telemetry"])
    assert set(stats) == set(small_lake["streams"])
    for key, s in small_lake["streams"].items():
        assert stats[key].received == s["sent"] - s["missing_seqs"], key


def test_latency_percentiles_match_python(spark, small_lake):
    # Oracle: latency of the first valid arrival of each message (what silver keeps).
    first = {}
    for d in small_lake["result"].deliveries:
        k = (d.aircraft_id, d.msg_type, d.seq)
        if not d.corrupt and (k not in first or d.recv_ts_ms < first[k].recv_ts_ms):
            first[k] = d
    latencies = defaultdict(list)
    for d in first.values():
        latencies[(d.aircraft_id, d.msg_type)].append(d.recv_ts_ms - d.event_ts_ms)

    stats = stats_by_stream(spark, small_lake["paths"]["telemetry"])
    for key, values in latencies.items():
        r = stats[key]
        assert r.latency_p50_ms == exact_percentile(values, 0.50), key
        assert r.latency_p95_ms == exact_percentile(values, 0.95), key
        assert r.latency_p99_ms == exact_percentile(values, 0.99), key
        assert r.latency_max_ms == max(values), key
        assert r.latency_p50_ms <= r.latency_p95_ms <= r.latency_p99_ms <= r.latency_max_ms


def test_rate_on_a_clean_link_equals_configured_rate(spark, tmp_path):
    from uav_lakehouse.bronze.ingest import ingest_landing
    from uav_lakehouse.generator.config import SimConfig
    from uav_lakehouse.generator.simulate import simulate
    from uav_lakehouse.generator.writer import write_run
    from uav_lakehouse.silver.build import build_silver

    cfg = replace(SimConfig(n_aircraft=1, duration_s=60, seed=1), faults=FaultConfig.none())
    write_run(simulate(cfg), cfg, tmp_path / "landing", "clean")
    bronze, telemetry = str(tmp_path / "bronze"), str(tmp_path / "telemetry")
    ingest_landing(spark, str(tmp_path / "landing"), bronze, str(tmp_path / "ckpt"))
    build_silver(spark, bronze, telemetry, str(tmp_path / "quarantine"))

    rates = {r.msg_type: r.received_rate_hz for r in stream_stats(spark, telemetry).collect()}
    assert rates == pytest.approx({m.msg_type: m.rate_hz for m in cfg.messages}, abs=0.01)
