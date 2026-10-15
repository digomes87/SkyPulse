from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from prometheus_client import REGISTRY
from pyspark.sql import SparkSession

from skypulse.common.config import Settings
from skypulse.observability.streaming import MetricsListener, push_metrics, record_progress
from skypulse.streaming.bronze import bronze_path, to_bronze
from skypulse.streaming.jobs import start_silver
from skypulse.streaming.silver import SILVER_TABLES
from skypulse.streaming.triggers import Trigger
from tests.unit.streaming.events import BASE, aircraft_state
from tests.unit.streaming.kafka_rows import kafka_frame


def value(name: str, query: str) -> float | None:
    return REGISTRY.get_sample_value(name, {"query": query})


def progress(name: str, behind: str, watermark: str) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        id="id-1",
        numInputRows=120,
        inputRowsPerSecond=40.0,
        processedRowsPerSecond=None,
        batchDuration=850,
        sources=[SimpleNamespace(metrics={"maxOffsetsBehindLatest": behind})],
        eventTime={"watermark": watermark},
    )


def test_progress_becomes_gauges_including_kafka_lag() -> None:
    ten_minutes_ago = (datetime.now(UTC) - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")

    record_progress(progress("bronze_test", "37", ten_minutes_ago))

    assert value("skypulse_stream_input_rows", "bronze_test") == 120
    assert value("skypulse_stream_processed_rows_per_second", "bronze_test") == 0
    assert value("skypulse_stream_offsets_behind_latest", "bronze_test") == 37
    assert value("skypulse_stream_watermark_delay_seconds", "bronze_test") == pytest.approx(
        600, abs=5
    )


def test_epoch_watermark_means_no_watermark_yet() -> None:
    record_progress(progress("no_watermark_test", "0", "1970-01-01T00:00:00.000Z"))

    assert value("skypulse_stream_watermark_delay_seconds", "no_watermark_test") is None


@pytest.mark.spark
def test_listener_records_real_micro_batches(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SKYPULSE_LAKE__ROOT_URI", tmp_path.as_uri())
    settings = Settings()
    to_bronze(
        kafka_frame(spark, "skypulse.raw.aircraft-states", [aircraft_state("aaaaaa", BASE)])
    ).write.format("delta").save(bronze_path(settings))
    listener = MetricsListener()
    spark.streams.addListener(listener)
    table = next(table for table in SILVER_TABLES if table.name == "aircraft_states")

    start_silver(spark, settings, table, Trigger(available_now=True)).awaitTermination()
    spark.streams.removeListener(listener)

    assert (
        REGISTRY.get_sample_value(
            "skypulse_stream_batches_total", {"query": "silver_aircraft_states"}
        )
        or 0
    ) >= 1


def test_push_only_with_a_pushgateway(monkeypatch: pytest.MonkeyPatch) -> None:
    pushed: list[str] = []
    monkeypatch.setattr(
        "skypulse.observability.streaming.push_to_gateway",
        lambda gateway, job, registry: pushed.append(job),
    )

    assert push_metrics(Settings(), "streaming_bronze") is False
    monkeypatch.setenv("SKYPULSE_METRICS__PUSHGATEWAY", "pushgateway:9091")
    assert push_metrics(Settings(), "streaming_bronze") is True
    assert pushed == ["streaming_bronze"]
