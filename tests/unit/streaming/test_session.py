import pytest
from pyspark.sql import SparkSession

from skypulse.common.config import RunMode, Settings
from skypulse.streaming.session import KAFKA_PACKAGE, S3A_PACKAGE, Connectors, _s3a_options


def test_connectors_only_pull_the_packages_a_job_needs() -> None:
    assert Connectors().packages() == []
    assert Connectors(kafka=True, s3a=True).packages() == [KAFKA_PACKAGE, S3A_PACKAGE]


def test_local_mode_points_s3a_at_localstack() -> None:
    options = _s3a_options(Settings())

    assert options["spark.hadoop.fs.s3a.endpoint"] == "http://localhost:4566"
    assert options["spark.hadoop.fs.s3a.path.style.access"] == "true"


def test_aws_mode_uses_the_default_s3a_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SKYPULSE_RUN_MODE", RunMode.AWS.value)

    assert _s3a_options(Settings()) == {}


@pytest.mark.spark
def test_session_has_delta_and_utc(spark: SparkSession) -> None:
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert "DeltaSparkSessionExtension" in str(spark.conf.get("spark.sql.extensions"))
