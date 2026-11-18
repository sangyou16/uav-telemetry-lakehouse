import pytest


@pytest.fixture(scope="session")
def spark():
    """One SparkSession for the whole test run (startup takes a few seconds)."""
    from uav_lakehouse.spark import get_spark  # imported lazily: generator tests don't need Spark

    session = get_spark("tests")
    yield session
    session.stop()
