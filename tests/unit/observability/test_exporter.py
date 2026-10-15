from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest
from deltalake import write_deltalake
from prometheus_client import REGISTRY

from skypulse.common.config import LakeLayer, RunMode, Settings
from skypulse.observability.exporter import Exporter, record_lake
from skypulse.observability.lake import LakeTable, last_commit, storage_options


def gauge(name: str, labels: dict[str, str] | None = None) -> float | None:
    return REGISTRY.get_sample_value(name, labels or {})


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("SKYPULSE_LAKE__ROOT_URI", tmp_path.as_uri())
    return Settings()


def write_table(settings: Settings, name: str) -> None:
    uri = LakeTable(LakeLayer.SILVER, name).uri(settings)
    write_deltalake(uri, pd.DataFrame({"value": [1]}))


def test_last_commit_reads_the_delta_log(settings: Settings) -> None:
    write_table(settings, "flight_schedules")

    moment = last_commit(LakeTable(LakeLayer.SILVER, "flight_schedules"), settings)

    assert moment is not None
    assert datetime.now(UTC) - moment < timedelta(minutes=1)
    assert last_commit(LakeTable(LakeLayer.SILVER, "missing"), settings) is None


def test_lake_probe_sets_commit_times_and_sla_breaches(settings: Settings) -> None:
    write_table(settings, "flight_schedules")

    record_lake(settings, datetime.now(UTC))

    assert gauge(
        "skypulse_table_last_commit_timestamp_seconds",
        {"layer": "silver", "table": "flight_schedules"},
    )
    assert gauge("skypulse_table_freshness_sla_breached", {"table": "flight_schedules"}) == 0
    assert gauge("skypulse_table_freshness_sla_breached", {"table": "aircraft_states"}) == 1


def test_s3_uris_and_options_for_localstack(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings()

    assert LakeTable(LakeLayer.SILVER, "x").uri(settings) == "s3://skypulse-silver/x"
    assert storage_options(settings)["AWS_ALLOW_HTTP"] == "true"
    monkeypatch.setenv("SKYPULSE_RUN_MODE", RunMode.AWS.value)
    assert "AWS_ENDPOINT_URL" not in storage_options(Settings())


class FakeSqs:
    def get_queue_url(self, QueueName: str) -> dict[str, str]:  # noqa: N803 - boto3 API
        return {"QueueUrl": f"http://sqs/{QueueName}"}

    def get_queue_attributes(self, QueueUrl: str, AttributeNames: list[str]) -> dict[str, Any]:  # noqa: N803
        return {
            "Attributes": {"ApproximateNumberOfMessages": "3" if "calculator" in QueueUrl else "0"}
        }


class FakeDynamo:
    def describe_table(self, TableName: str) -> dict[str, Any]:  # noqa: N803 - boto3 API
        return {"Table": {"ItemCount": 26}}


class FakeConsumer:
    def list_topics(self, topic: str, timeout: float) -> SimpleNamespace:
        return SimpleNamespace(topics={topic: SimpleNamespace(partitions={0: None, 1: None})})

    def get_watermark_offsets(self, partition: Any, timeout: float) -> tuple[int, int]:
        return (2, 7)


def test_collect_once_fills_queue_topic_and_claim_gauges(settings: Settings) -> None:
    Exporter(settings, FakeSqs(), FakeDynamo(), FakeConsumer()).collect_once()

    assert gauge("skypulse_dlq_messages", {"queue": "eu261-calculator-dlq"}) == 3
    assert gauge("skypulse_dlq_messages", {"queue": "notifications-dlq"}) == 0
    assert gauge("skypulse_kafka_topic_messages", {"topic": "skypulse.dlq.raw"}) == 10
    assert gauge("skypulse_eu261_claims") == 26


def test_a_failing_probe_does_not_stop_the_others(settings: Settings) -> None:
    class BrokenSqs(FakeSqs):
        def get_queue_url(self, QueueName: str) -> dict[str, str]:  # noqa: N803
            raise ConnectionError("sqs down")

    Exporter(settings, BrokenSqs(), FakeDynamo(), FakeConsumer()).collect_once()

    assert gauge("skypulse_eu261_claims") == 26
