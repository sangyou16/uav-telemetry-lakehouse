"""A tiny, invented flight model: each aircraft flies a circle.

The physics only has to be plausible enough that payload values move
smoothly over time. Nothing here mirrors a real protocol.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass

METERS_PER_DEG_LAT = 111_320.0

# Open ocean near 0N 30W: clearly synthetic, no real place.
AREA_CENTER = (0.0, -30.0)


@dataclass(frozen=True)
class Aircraft:
    aircraft_id: str
    center_lat: float
    center_lon: float
    radius_m: float
    alt_m: float
    speed_mps: float
    phase_rad: float  # where on the circle it starts
    battery_start_pct: float
    drain_pct_per_min: float


def make_fleet(n: int, rng: random.Random) -> list[Aircraft]:
    lat0, lon0 = AREA_CENTER
    return [
        Aircraft(
            aircraft_id=f"UAV-{i:03d}",
            center_lat=lat0 + rng.uniform(-0.5, 0.5),
            center_lon=lon0 + rng.uniform(-0.5, 0.5),
            radius_m=rng.uniform(500, 3000),
            alt_m=rng.uniform(300, 1500),
            speed_mps=rng.uniform(20, 40),
            phase_rad=rng.uniform(0, 2 * math.pi),
            battery_start_pct=rng.uniform(85, 100),
            drain_pct_per_min=rng.uniform(0.5, 1.5),
        )
        for i in range(1, n + 1)
    ]


def _angle(ac: Aircraft, t_s: float) -> float:
    """Angle on the circle after t seconds (angular speed = v / r)."""
    return ac.phase_rad + (ac.speed_mps / ac.radius_m) * t_s


def _heading_deg(theta: float) -> float:
    # Position on the circle is (east, north) = r*(cos θ, sin θ).
    # Velocity is its derivative: (-sin θ, cos θ). Heading is measured from north.
    east_v, north_v = -math.sin(theta), math.cos(theta)
    return math.degrees(math.atan2(east_v, north_v)) % 360


def _battery_pct(ac: Aircraft, t_s: float) -> float:
    return max(0.0, ac.battery_start_pct - ac.drain_pct_per_min * t_s / 60)


def build_payload(msg_type: str, ac: Aircraft, t_s: float, rng: random.Random) -> dict:
    """Payload for one message, at t_s seconds after the run started."""
    theta = _angle(ac, t_s)

    if msg_type == "position":
        east_m, north_m = ac.radius_m * math.cos(theta), ac.radius_m * math.sin(theta)
        lat = ac.center_lat + north_m / METERS_PER_DEG_LAT
        lon = ac.center_lon + east_m / (METERS_PER_DEG_LAT * math.cos(math.radians(lat)))
        return {
            "lat_deg": round(lat, 7),
            "lon_deg": round(lon, 7),
            "alt_m": round(ac.alt_m + rng.gauss(0, 2), 2),
            "ground_speed_mps": round(ac.speed_mps + rng.gauss(0, 0.3), 2),
            "heading_deg": round(_heading_deg(theta), 2),
        }

    if msg_type == "attitude":
        # Steady left turn: constant bank angle, from tan(bank) = v^2 / (r * g).
        bank = math.degrees(math.atan(ac.speed_mps**2 / (ac.radius_m * 9.81)))
        return {
            "roll_deg": round(-bank + rng.gauss(0, 0.5), 2),
            "pitch_deg": round(rng.gauss(2, 0.5), 2),
            "yaw_deg": round(_heading_deg(theta), 2),
        }

    if msg_type == "battery":
        pct = _battery_pct(ac, t_s)
        return {
            "voltage_v": round(19.0 + 6.0 * pct / 100 + rng.gauss(0, 0.05), 3),
            "current_a": round(rng.gauss(15, 1.5), 2),
            "remaining_pct": round(pct, 2),
        }

    if msg_type == "heartbeat":
        return {
            "mode": "AUTO",
            "armed": True,
            "health": "OK" if _battery_pct(ac, t_s) > 20 else "LOW_BATTERY",
        }

    raise ValueError(f"unknown msg_type: {msg_type!r}")
