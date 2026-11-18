"""Turn a SimConfig into delivered messages plus ground-truth counts.

Pipeline for each (aircraft, msg_type) stream:

    send times -> payloads -> [drop?] -> latency (+late, +buffer) -> [duplicate?] -> [corrupt?]

Every fault is counted in StreamStats so later layers can be checked
against the truth.
"""

from __future__ import annotations

import random
from dataclasses import asdict, dataclass

from .config import SimConfig
from .fleet import build_payload, make_fleet

Window = tuple[float, float]  # (start_s, end_s) relative to run start
StreamKey = tuple[str, str]  # (aircraft_id, msg_type)


@dataclass
class Delivery:
    """One line the ground receiver writes."""

    aircraft_id: str
    msg_type: str
    seq: int
    event_ts_ms: int
    recv_ts_ms: int
    payload: dict
    corrupt: bool = False  # writer will truncate this line

    def to_record(self) -> dict:
        return {
            "schema_version": 1,
            "aircraft_id": self.aircraft_id,
            "msg_type": self.msg_type,
            "seq": self.seq,
            "event_ts_ms": self.event_ts_ms,
            "recv_ts_ms": self.recv_ts_ms,
            "payload": self.payload,
        }


@dataclass
class StreamStats:
    sent: int = 0
    dropped_random: int = 0
    dropped_outage: int = 0
    duplicates: int = 0
    late: int = 0
    buffered: int = 0
    corrupted_lines: int = 0
    missing_seqs: int = 0  # seqs with no valid copy delivered
    detectable_missing: int = 0  # ...of those, the ones between first and last valid seq

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class SimResult:
    deliveries: list[Delivery]
    stats: dict[StreamKey, StreamStats]


def _random_windows(
    rng: random.Random, count: int, length_s: tuple[float, float], duration_s: float
) -> list[Window]:
    windows = []
    for _ in range(count):
        length = rng.uniform(*length_s)
        start = rng.uniform(0, max(0.0, duration_s - length))
        windows.append((start, start + length))
    return windows


def _window_containing(t_s: float, windows: list[Window]) -> Window | None:
    for w in windows:
        if w[0] <= t_s < w[1]:
            return w
    return None


def _send_times(rate_hz: float, duration_s: float, rng: random.Random) -> list[float]:
    """Evenly spaced send times, with a random phase so streams don't line up."""
    period = 1.0 / rate_hz
    t = rng.uniform(0, period)
    times = []
    while t < duration_s:
        times.append(t)
        t += period
    return times


def _count_missing(deliveries: list[Delivery], stats: StreamStats) -> None:
    valid_seqs = {d.seq for d in deliveries if not d.corrupt}
    stats.missing_seqs = stats.sent - len(valid_seqs)
    if valid_seqs:
        span = max(valid_seqs) - min(valid_seqs) + 1
        stats.detectable_missing = span - len(valid_seqs)


def simulate(cfg: SimConfig) -> SimResult:
    f = cfg.faults
    fleet = make_fleet(cfg.n_aircraft, random.Random(cfg.seed))
    deliveries: list[Delivery] = []
    all_stats: dict[StreamKey, StreamStats] = {}

    for ac in fleet:
        # One RNG per aircraft: adding aircraft doesn't change existing ones.
        rng = random.Random(f"{cfg.seed}:{ac.aircraft_id}")
        outages = _random_windows(rng, f.outages_per_aircraft, f.outage_duration_s, cfg.duration_s)
        buffers = _random_windows(
            rng, f.buffer_windows_per_aircraft, f.buffer_duration_s, cfg.duration_s
        )

        for spec in cfg.messages:
            stats = StreamStats()
            stream: list[Delivery] = []

            for seq, t_s in enumerate(_send_times(spec.rate_hz, cfg.duration_s, rng)):
                stats.sent += 1
                event_ts_ms = cfg.start_ts_ms + round(t_s * 1000)
                payload = build_payload(spec.msg_type, ac, t_s, rng)

                # 1. Lost on the way?
                if _window_containing(t_s, outages):
                    stats.dropped_outage += 1
                    continue
                if rng.random() < f.drop_prob:
                    stats.dropped_random += 1
                    continue

                # 2. How long did it take?
                latency_ms = cfg.base_latency_ms
                if cfg.jitter_mean_ms > 0:
                    latency_ms += rng.expovariate(1 / cfg.jitter_mean_ms)
                if rng.random() < f.late_prob:
                    latency_ms += rng.uniform(*f.late_delay_ms)
                    stats.late += 1
                recv_ts_ms = event_ts_ms + round(latency_ms)

                # Store-and-forward: held until the window ends, then flushed.
                if w := _window_containing(t_s, buffers):
                    flush_ts_ms = cfg.start_ts_ms + round(w[1] * 1000) + rng.randint(0, 200)
                    recv_ts_ms = max(recv_ts_ms, flush_ts_ms)
                    stats.buffered += 1

                d = Delivery(ac.aircraft_id, spec.msg_type, seq, event_ts_ms, recv_ts_ms, payload)
                stream.append(d)

                # 3. Delivered twice? Same message, later receive time.
                if rng.random() < f.duplicate_prob:
                    dup_recv = recv_ts_ms + rng.randint(10, 500)
                    stream.append(
                        Delivery(d.aircraft_id, d.msg_type, d.seq, d.event_ts_ms, dup_recv, payload)
                    )
                    stats.duplicates += 1

            # 4. Some delivered lines arrive mangled.
            for d in stream:
                if rng.random() < f.corrupt_prob:
                    d.corrupt = True
                    stats.corrupted_lines += 1

            _count_missing(stream, stats)
            deliveries.extend(stream)
            all_stats[(ac.aircraft_id, spec.msg_type)] = stats

    return SimResult(deliveries, all_stats)
