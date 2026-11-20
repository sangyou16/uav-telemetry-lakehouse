import pytest


@pytest.fixture(scope="session")
def spark():
    """One SparkSession for the whole test run (startup takes a few seconds)."""
    from uav_lakehouse.spark import get_spark  # imported lazily: generator tests don't need Spark

    session = get_spark("tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def small_lake(spark, tmp_path_factory):
    """Generator -> bronze -> silver -> gaps, built once and shared (read-only!)."""
    import json

    from uav_lakehouse.bronze.ingest import ingest_landing
    from uav_lakehouse.generator.config import SimConfig
    from uav_lakehouse.generator.simulate import simulate
    from uav_lakehouse.generator.writer import write_run
    from uav_lakehouse.silver.build import build_silver
    from uav_lakehouse.silver.gaps import build_gaps

    cfg = SimConfig(n_aircraft=2, duration_s=120, seed=3)
    root = tmp_path_factory.mktemp("small_lake")
    result = simulate(cfg)
    run_dir = write_run(result, cfg, root / "landing", "run-a")
    p = {name: str(root / name) for name in ("bronze", "telemetry", "quarantine", "gaps")}
    ingest_landing(spark, str(root / "landing"), p["bronze"], str(root / "ckpt"))
    build_silver(spark, p["bronze"], p["telemetry"], p["quarantine"])
    try:
        build_gaps(spark, p["telemetry"], p["gaps"])
    except NotImplementedError:
        p["gaps"] = None  # gaps.sql not written yet
    manifest = json.loads((run_dir / "_manifest.json").read_text())
    streams = {(s["aircraft_id"], s["msg_type"]): s for s in manifest["streams"]}
    return {"cfg": cfg, "result": result, "paths": p, "streams": streams, "root": root}
