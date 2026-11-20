"""The whole pipeline with catalog table names instead of paths.

On Databricks the tables are Unity Catalog names (workspace.uav.silver_telemetry).
Locally the same three-part names work against Spark's own catalog, which is the
closest stand-in we can test in CI.
"""

import json

from uav_lakehouse.bronze.ingest import ingest_landing
from uav_lakehouse.delta_utils import read_delta
from uav_lakehouse.generator.config import SimConfig
from uav_lakehouse.generator.simulate import simulate
from uav_lakehouse.generator.writer import write_run
from uav_lakehouse.gold.build import build_gold
from uav_lakehouse.paths import catalog_tables
from uav_lakehouse.silver.build import build_silver
from uav_lakehouse.silver.gaps import build_gaps

CFG = SimConfig(n_aircraft=2, duration_s=60, seed=5)


def test_pipeline_with_table_names(spark, tmp_path):
    schema = "spark_catalog.uav_catalog_test"
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema} LOCATION '{tmp_path / 'warehouse'}'")
    t = catalog_tables(schema, str(tmp_path / "volume"))

    run_dir = write_run(simulate(CFG), CFG, tmp_path / "landing", "run-a")
    totals = json.loads((run_dir / "_manifest.json").read_text())["totals"]
    lines = sum(len(p.read_text().splitlines()) for p in run_dir.glob("batch-*.jsonl"))

    assert (
        ingest_landing(spark, str(tmp_path / "landing"), t["bronze"], t["bronze_checkpoint"])
        == lines
    )
    assert (
        ingest_landing(spark, str(tmp_path / "landing"), t["bronze"], t["bronze_checkpoint"]) == 0
    )

    counts = build_silver(spark, t["bronze"], t["telemetry"], t["quarantine"])
    assert counts["telemetry_inserted"] == totals["sent"] - totals["missing_seqs"]
    assert counts["quarantine_inserted"] == totals["corrupted_lines"]

    build_gaps(spark, t["telemetry"], t["gaps"])
    assert build_gold(spark, t["telemetry"], t["gaps"], t["stream_health"]) == 2 * len(CFG.messages)

    missing = read_delta(spark, t["stream_health"]).agg({"missing": "sum"}).first()[0]
    assert missing == totals["detectable_missing"]
    assert spark.catalog.tableExists(f"{schema}.gold_stream_health")
    spark.sql(f"DROP SCHEMA {schema} CASCADE")
