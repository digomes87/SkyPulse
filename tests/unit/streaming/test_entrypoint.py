from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQueryManager

from skypulse.common.config import Settings
from skypulse.observability.streaming import MetricsListener
from skypulse.streaming.__main__ import Stage, instrument, parse_arguments
from skypulse.streaming.jobs import wait_for_table


def test_bronze_needs_kafka_and_silver_does_not() -> None:
    settings = Settings()

    assert Stage.BRONZE.connectors(settings).kafka
    assert not Stage.SILVER.connectors(settings).kafka


def test_s3a_is_only_loaded_when_the_lake_lives_in_buckets(monkeypatch: pytest.MonkeyPatch) -> None:
    assert Stage.SILVER.connectors(Settings()).s3a

    monkeypatch.setenv("SKYPULSE_LAKE__ROOT_URI", "file:///tmp/lake")

    assert not Stage.SILVER.connectors(Settings()).s3a


def test_cli_arguments() -> None:
    arguments = parse_arguments(["silver", "--available-now", "--master", "local[2]"])

    assert arguments.stage is Stage.SILVER
    assert arguments.available_now
    assert arguments.master == "local[2]"


@pytest.mark.spark
def test_instrument_registers_the_metrics_listener(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    started: list[int] = []
    added: list[object] = []
    monkeypatch.setattr("skypulse.observability.metrics.start_http_server", started.append)
    monkeypatch.setattr(
        StreamingQueryManager, "addListener", lambda self, listener: added.append(listener)
    )
    monkeypatch.setenv("SKYPULSE_METRICS__PORT", "9466")

    instrument(spark, Settings())

    assert started == [9466]
    assert [type(listener) for listener in added] == [MetricsListener]


@pytest.mark.spark
def test_waiting_for_a_missing_table_times_out(spark: SparkSession, tmp_path: Path) -> None:
    waits: list[float] = []

    with pytest.raises(TimeoutError, match="did not appear"):
        wait_for_table(spark, str(tmp_path / "nothing"), timeout_seconds=10, sleep=waits.append)

    assert waits == [5.0, 5.0]
