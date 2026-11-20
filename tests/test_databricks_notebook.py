"""Run notebooks/databricks_pipeline.py top to bottom on local Spark.

Databricks provides `spark`, `dbutils` and `display`; here they are small stand-ins.
This catches typos and wiring mistakes before anyone opens the notebook in a workspace.
"""

from pathlib import Path

NOTEBOOK = Path(__file__).resolve().parents[1] / "notebooks" / "databricks_pipeline.py"


class Widgets:
    def __init__(self, values):
        self.values = values

    def text(self, name, default):
        self.values.setdefault(name, default)

    def get(self, name):
        return self.values[name]


class DbUtils:
    def __init__(self, values):
        self.widgets = Widgets(values)


class LocalSpark:
    """Real local Spark, minus Unity Catalog-only SQL (volumes)."""

    def __init__(self, spark):
        self._spark = spark

    def sql(self, query, *args, **kwargs):
        if query.strip().upper().startswith("CREATE VOLUME"):
            return None
        return self._spark.sql(query, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._spark, name)


def test_notebook_runs_end_to_end(spark, tmp_path, monkeypatch):
    schema = "uav_notebook_test"
    spark.sql(f"DROP SCHEMA IF EXISTS spark_catalog.{schema} CASCADE")
    spark.sql(f"CREATE SCHEMA spark_catalog.{schema} LOCATION '{tmp_path / 'warehouse'}'")
    monkeypatch.chdir(NOTEBOOK.parent)  # Databricks runs a notebook from its own folder

    shown = []
    widgets = {
        "catalog": "spark_catalog",
        "schema": schema,
        "aircraft": "2",
        "duration_s": "60",
        "seed": "11",
        "volume_root": str(tmp_path / "volume"),
    }
    env = {
        "spark": LocalSpark(spark),
        "dbutils": DbUtils(widgets),
        "display": lambda df: shown.append(df.collect()),
    }
    exec(compile(NOTEBOOK.read_text(), str(NOTEBOOK), "exec"), env)  # asserts inside

    assert len(shown) == 2 and len(shown[0]) == 8  # gold table: 2 aircraft x 4 msg types
    spark.sql(f"DROP SCHEMA spark_catalog.{schema} CASCADE")
