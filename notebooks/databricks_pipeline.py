# Databricks notebook source
# MAGIC %md
# MAGIC # UAV telemetry lakehouse on Databricks
# MAGIC
# MAGIC Runs the same generator → bronze → silver → gold code as the laptop and S3 versions,
# MAGIC with tables in **Unity Catalog** and files in a **volume**. Works on Databricks
# MAGIC Free Edition (serverless). All data is synthetic.
# MAGIC
# MAGIC **Run:** open this notebook from a Git folder of the repo and click **Run all**.
# MAGIC
# MAGIC | what | where |
# MAGIC |---|---|
# MAGIC | landing files + bronze checkpoint | volume `/Volumes/<catalog>/<schema>/raw` |
# MAGIC | tables | `<catalog>.<schema>.` + `bronze_telemetry_raw`, `silver_telemetry`, |
# MAGIC | | `silver_quarantine`, `silver_gaps`, `gold_stream_health` |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "uav")
dbutils.widgets.text("aircraft", "5")
dbutils.widgets.text("duration_s", "600")
dbutils.widgets.text("seed", "42")
dbutils.widgets.text("volume_root", "")  # leave empty on Databricks

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = f"{CATALOG}.{dbutils.widgets.get('schema')}"
VOLUME = (
    dbutils.widgets.get("volume_root") or f"/Volumes/{CATALOG}/{dbutils.widgets.get('schema')}/raw"
)
RUN_ID = f"run-{dbutils.widgets.get('seed')}"

# COMMAND ----------

# Make the repo's code importable: this notebook lives in <repo>/notebooks.
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(os.getcwd()).parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from uav_lakehouse.bronze.ingest import ingest_landing  # noqa: E402
from uav_lakehouse.generator.config import SimConfig  # noqa: E402
from uav_lakehouse.generator.simulate import simulate  # noqa: E402
from uav_lakehouse.generator.writer import write_run  # noqa: E402
from uav_lakehouse.gold.build import build_gold  # noqa: E402
from uav_lakehouse.paths import catalog_tables  # noqa: E402
from uav_lakehouse.silver.build import build_silver  # noqa: E402
from uav_lakehouse.silver.gaps import build_gaps  # noqa: E402

T = catalog_tables(SCHEMA, VOLUME)
print(T)

# COMMAND ----------

# MAGIC %md ## 1. Schema, volume and synthetic landing files

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.raw")

cfg = SimConfig(
    n_aircraft=int(dbutils.widgets.get("aircraft")),
    duration_s=float(dbutils.widgets.get("duration_s")),
    seed=int(dbutils.widgets.get("seed")),
)
run_dir = write_run(simulate(cfg), cfg, Path(VOLUME) / "landing", RUN_ID)
manifest = json.loads((run_dir / "_manifest.json").read_text())
print(f"wrote {manifest['lines']:,} lines to {run_dir}")

# COMMAND ----------

# MAGIC %md ## 2. Bronze: raw lines, incremental (run this cell twice: the second run appends 0)

# COMMAND ----------

appended = ingest_landing(spark, f"{VOLUME}/landing", T["bronze"], T["bronze_checkpoint"])
print(f"appended {appended:,} rows to {T['bronze']}")

# COMMAND ----------

# MAGIC %md ## 3. Silver: parse, quarantine, dedupe (MERGE), gaps

# COMMAND ----------

counts = build_silver(spark, T["bronze"], T["telemetry"], T["quarantine"])
n_gaps = build_gaps(spark, T["telemetry"], T["gaps"])
print(counts, f"gaps={n_gaps:,}")

# COMMAND ----------

# MAGIC %md ## 4. Gold: per-stream loss %, rate, latency percentiles

# COMMAND ----------

build_gold(spark, T["telemetry"], T["gaps"], T["stream_health"])
display(spark.table(T["stream_health"]).orderBy("aircraft_id", "msg_type"))

# COMMAND ----------

# MAGIC %md ## 5. Check against the generator's ground truth

# COMMAND ----------

truth = manifest["totals"]
got = {
    "silver rows": spark.table(T["telemetry"]).count(),
    "quarantined": spark.table(T["quarantine"]).count(),
    "missing (gold)": spark.table(T["stream_health"]).agg({"missing": "sum"}).first()[0],
}
expected = {
    "silver rows": truth["sent"] - truth["missing_seqs"],
    "quarantined": truth["corrupted_lines"],
    "missing (gold)": truth["detectable_missing"],
}
for k in got:
    status = "OK" if got[k] == expected[k] else "MISMATCH"
    print(f"{k:<16} got {got[k]:>8,}   expected {expected[k]:>8,}   {status}")
assert got == expected

# COMMAND ----------

# MAGIC %md ## 6. Example SQL on the gold table

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT aircraft_id,
           SUM(received)                                              AS received,
           SUM(missing)                                               AS missing,
           ROUND(100.0 * SUM(missing) / (SUM(received) + SUM(missing)), 2) AS loss_pct,
           MAX(latency_p99_ms)                                        AS worst_p99_ms
    FROM {T["stream_health"]}
    GROUP BY aircraft_id
    ORDER BY loss_pct DESC
""")
)

# COMMAND ----------

# MAGIC %md ## Clean up (optional)
# MAGIC Uncomment and run to delete everything this notebook created.

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
