import json
from collections import defaultdict
from dataclasses import replace
from itertools import pairwise

import pytest

from uav_lakehouse.generator.config import FaultConfig, MessageSpec, SimConfig
from uav_lakehouse.generator.simulate import simulate
from uav_lakehouse.generator.writer import write_run

SMALL = SimConfig(n_aircraft=3, duration_s=60, seed=7)
CLEAN = replace(SMALL, faults=FaultConfig.none())


def read_run(run_dir):
    """Return (parsed records, number of unparseable lines) from all batch files."""
    records, bad = [], 0
    for path in sorted(run_dir.glob("batch-*.jsonl")):
        for line in path.read_text().splitlines():
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                bad += 1
    return records, bad


def test_same_seed_same_output():
    a, b = simulate(SMALL), simulate(SMALL)
    assert [d.to_record() for d in a.deliveries] == [d.to_record() for d in b.deliveries]


def test_different_seed_different_output():
    a, b = simulate(SMALL), simulate(replace(SMALL, seed=8))
    assert [d.to_record() for d in a.deliveries] != [d.to_record() for d in b.deliveries]


def test_clean_link_delivers_every_message_once():
    result = simulate(CLEAN)
    keys = [(d.aircraft_id, d.msg_type, d.seq) for d in result.deliveries]
    assert len(keys) == len(set(keys)) == sum(s.sent for s in result.stats.values())
    assert all(s.missing_seqs == 0 for s in result.stats.values())


@pytest.mark.parametrize("spec", SMALL.messages, ids=lambda s: s.msg_type)
def test_rates_match_config(spec: MessageSpec):
    result = simulate(CLEAN)
    sent = result.stats[("UAV-001", spec.msg_type)].sent
    expected = spec.rate_hz * CLEAN.duration_s
    assert abs(sent - expected) <= 1


def test_latency_is_positive_and_bounded_on_clean_link():
    for d in simulate(CLEAN).deliveries:
        latency = d.recv_ts_ms - d.event_ts_ms
        assert CLEAN.base_latency_ms <= latency < 1000


def test_sequences_are_contiguous_per_stream():
    by_stream = defaultdict(list)
    for d in simulate(CLEAN).deliveries:
        by_stream[(d.aircraft_id, d.msg_type)].append(d.seq)
    for seqs in by_stream.values():
        assert sorted(seqs) == list(range(len(seqs)))


def test_outage_drops_a_contiguous_block():
    only_outage = replace(
        CLEAN, faults=replace(FaultConfig.none(), outages_per_aircraft=1, outage_duration_s=(5, 5))
    )
    result = simulate(only_outage)
    stats = result.stats[("UAV-001", "attitude")]
    # 5 s at 20 Hz = 100 messages (+/- 1 for phase).
    assert 99 <= stats.dropped_outage <= 101
    seqs = sorted(
        d.seq for d in result.deliveries if d.aircraft_id == "UAV-001" and d.msg_type == "attitude"
    )
    gaps = [b - a - 1 for a, b in pairwise(seqs) if b - a > 1]
    assert len(gaps) <= 1  # one hole (none if the outage hit the very start)


def test_duplicates_share_key_and_payload():
    dup_only = replace(CLEAN, faults=replace(FaultConfig.none(), duplicate_prob=0.2))
    result = simulate(dup_only)
    seen = {}
    dups = 0
    for d in result.deliveries:
        key = (d.aircraft_id, d.msg_type, d.seq)
        if key in seen:
            first = seen[key]
            assert (first.event_ts_ms, first.payload) == (d.event_ts_ms, d.payload)
            assert d.recv_ts_ms > first.recv_ts_ms
            dups += 1
        else:
            seen[key] = d
    assert dups == sum(s.duplicates for s in result.stats.values()) > 0


def test_files_are_in_arrival_order_and_contain_out_of_order_events(tmp_path):
    run_dir = write_run(simulate(SMALL), SMALL, tmp_path, "t")
    records, _ = read_run(run_dir)
    recv = [r["recv_ts_ms"] for r in records]
    assert recv == sorted(recv)
    # Late and buffered messages mean event time is NOT sorted within a stream.
    att = [
        r["seq"] for r in records if r["aircraft_id"] == "UAV-001" and r["msg_type"] == "attitude"
    ]
    assert att != sorted(att)


def test_manifest_matches_files(tmp_path):
    """Recompute the ground truth from the files alone. Silver must do the same."""
    run_dir = write_run(simulate(SMALL), SMALL, tmp_path, "t")
    manifest = json.loads((run_dir / "_manifest.json").read_text())
    records, bad_lines = read_run(run_dir)

    assert bad_lines == manifest["totals"]["corrupted_lines"]
    assert len(records) + bad_lines == manifest["lines"]

    valid = defaultdict(set)
    for r in records:
        valid[(r["aircraft_id"], r["msg_type"])].add(r["seq"])

    for s in manifest["streams"]:
        seqs = valid[(s["aircraft_id"], s["msg_type"])]
        gaps_from_files = (max(seqs) - min(seqs) + 1) - len(seqs)
        assert gaps_from_files == s["detectable_missing"], s
