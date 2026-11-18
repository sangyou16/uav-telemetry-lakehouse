"""Settings for the synthetic telemetry generator.

Everything is a frozen dataclass so a run is fully described by one object,
which is also written into the manifest for reproducibility.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# 2026-01-01T00:00:00Z. A fixed start keeps runs reproducible.
DEFAULT_START_TS_MS = 1_767_225_600_000


@dataclass(frozen=True)
class MessageSpec:
    """One message type and how often each aircraft sends it."""

    msg_type: str
    rate_hz: float


DEFAULT_MESSAGES: tuple[MessageSpec, ...] = (
    MessageSpec("heartbeat", 1.0),
    MessageSpec("position", 10.0),
    MessageSpec("attitude", 20.0),
    MessageSpec("battery", 0.5),
)


@dataclass(frozen=True)
class FaultConfig:
    """Knobs for the faults injected between aircraft and ground."""

    drop_prob: float = 0.005  # single random message lost
    duplicate_prob: float = 0.01  # message delivered twice
    late_prob: float = 0.01  # extra delay -> arrives out of order
    late_delay_ms: tuple[int, int] = (200, 2000)
    corrupt_prob: float = 0.001  # delivered line is truncated (invalid JSON)
    outages_per_aircraft: int = 1  # windows where everything is lost
    outage_duration_s: tuple[float, float] = (2.0, 8.0)
    buffer_windows_per_aircraft: int = 1  # windows held, then flushed at once
    buffer_duration_s: tuple[float, float] = (3.0, 10.0)

    @classmethod
    def none(cls) -> FaultConfig:
        """A perfect link: no faults at all. Handy for tests."""
        return cls(
            drop_prob=0.0,
            duplicate_prob=0.0,
            late_prob=0.0,
            corrupt_prob=0.0,
            outages_per_aircraft=0,
            buffer_windows_per_aircraft=0,
        )


@dataclass(frozen=True)
class SimConfig:
    n_aircraft: int = 5
    duration_s: float = 600.0
    seed: int = 42
    start_ts_ms: int = DEFAULT_START_TS_MS
    messages: tuple[MessageSpec, ...] = DEFAULT_MESSAGES
    faults: FaultConfig = field(default_factory=FaultConfig)
    base_latency_ms: float = 40.0  # every message takes at least this long
    jitter_mean_ms: float = 15.0  # plus exponential jitter with this mean
