from pathlib import Path

import pytest

from uav_lakehouse.paths import lake_tables, normalize
from uav_lakehouse.upload import split_uri


def test_normalize_local_and_s3():
    assert normalize("data/lake") == str(Path("data/lake").resolve())
    assert normalize("s3://lake/x/") == "s3a://lake/x"
    assert normalize("s3a://lake/x") == "s3a://lake/x"


def test_lake_tables_on_s3():
    t = lake_tables("s3a://bucket/lake")
    assert t["bronze"] == "s3a://bucket/lake/bronze/telemetry_raw"
    assert t["stream_health"] == "s3a://bucket/lake/gold/stream_health"


def test_split_uri():
    assert split_uri("s3a://lake/landing/run-1") == ("lake", "landing/run-1")
    assert split_uri("s3://lake") == ("lake", "")
    with pytest.raises(ValueError):
        split_uri("data/landing")


def test_hadoop_aws_matches_pyspark_hadoop():
    """hadoop-aws must be the exact Hadoop version inside pyspark, or s3a fails at runtime."""
    import pyspark

    from uav_lakehouse.spark import HADOOP_AWS_PACKAGE

    jars = Path(pyspark.__file__).parent / "jars"
    bundled = {
        p.name.removeprefix("hadoop-client-api-").removesuffix(".jar")
        for p in jars.glob("hadoop-client-api-*.jar")
    }
    assert HADOOP_AWS_PACKAGE.rsplit(":", 1)[1] in bundled, bundled
