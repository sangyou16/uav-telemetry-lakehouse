"""Where each table lives. A lake root can be a local folder or an s3a:// URI."""

from __future__ import annotations

from pathlib import Path


def normalize(location: str | Path) -> str:
    """Absolute local path, or an s3a:// URI ("s3://" is accepted and rewritten)."""
    loc = str(location)
    if loc.startswith("s3://"):
        loc = "s3a://" + loc[len("s3://") :]
    if "://" in loc:
        return loc.rstrip("/")
    return str(Path(loc).resolve())


def lake_tables(lake: str | Path) -> dict[str, str]:
    """Every table and checkpoint location under one lake root."""
    root = normalize(lake)
    return {
        "bronze": f"{root}/bronze/telemetry_raw",
        "bronze_checkpoint": f"{root}/_checkpoints/bronze",
        "telemetry": f"{root}/silver/telemetry",
        "quarantine": f"{root}/silver/quarantine",
        "gaps": f"{root}/silver/gaps",
        "stream_health": f"{root}/gold/stream_health",
    }


def catalog_tables(schema: str, volume_root: str) -> dict[str, str]:
    """Same keys as lake_tables(), but as catalog table names (e.g. Unity Catalog).

    schema:      "workspace.uav"  ->  tables like workspace.uav.silver_telemetry
    volume_root: a path for things that must be files: the streaming checkpoint.
    """
    return {
        "bronze": f"{schema}.bronze_telemetry_raw",
        "bronze_checkpoint": f"{volume_root.rstrip('/')}/_checkpoints/bronze",
        "telemetry": f"{schema}.silver_telemetry",
        "quarantine": f"{schema}.silver_quarantine",
        "gaps": f"{schema}.silver_gaps",
        "stream_health": f"{schema}.gold_stream_health",
    }
