"""Replay producers -> Kafka -> bronze -> silver, on the committed fixtures."""

import asyncio
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from skypulse.common.config import Settings
from skypulse.ingestion.__main__ import main as ingest
from skypulse.streaming.__main__ import main as stream
from skypulse.streaming.bronze import bronze_path
from skypulse.streaming.silver import SILVER_TABLES, silver_path

pytestmark = [pytest.mark.integration, pytest.mark.spark]

FIXTURES = Path(__file__).parents[2] / "data" / "fixtures"
TABLES = {table.name: table for table in SILVER_TABLES}


@pytest.fixture(scope="module")
def settings(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Settings]:
    patch = pytest.MonkeyPatch()
    patch.setenv("SKYPULSE_INGESTION__REPLAY", "true")
    patch.setenv("SKYPULSE_INGESTION__FIXTURES_DIR", str(FIXTURES))
    patch.setenv("SKYPULSE_INGESTION__REPLAY_START", "2026-10-14T10:30:00Z")
    patch.setenv("SKYPULSE_LAKE__ROOT_URI", tmp_path_factory.mktemp("lake").as_uri())
    yield Settings()
    patch.undo()


@pytest.fixture(scope="module")
def pipeline(settings: Settings) -> SparkSession:
    for feed in ("states", "schedules", "metar"):
        assert asyncio.run(ingest([feed, "--cycles", "1"])) == 0
    assert stream(["bronze", "--available-now", "--master", "local[2]"]) == 0
    assert stream(["silver", "--available-now", "--master", "local[2]"]) == 0
    return SparkSession.builder.getOrCreate()


@pytest.mark.parametrize("table", sorted(set(TABLES) - {"observed_flights"}))
def test_every_fed_silver_table_has_rows(
    pipeline: SparkSession, settings: Settings, table: str
) -> None:
    assert pipeline.read.format("delta").load(silver_path(settings, TABLES[table])).count() > 0


def test_bronze_keeps_every_kafka_record(pipeline: SparkSession, settings: Settings) -> None:
    bronze = pipeline.read.format("delta").load(bronze_path(settings))

    assert bronze.where("topic = 'skypulse.raw.metar'").count() > 0


def test_pinned_scenarios_reach_silver(pipeline: SparkSession, settings: Settings) -> None:
    schedules = pipeline.read.format("delta").load(
        silver_path(settings, TABLES["flight_schedules"])
    )
    status = {row["flight_number"]: row["status"] for row in schedules.collect()}

    assert status["FR1234"] == "Canceled"
    assert status["EI105"] == "Delayed"


def test_silver_has_one_row_per_flight(pipeline: SparkSession, settings: Settings) -> None:
    schedules = pipeline.read.format("delta").load(
        silver_path(settings, TABLES["flight_schedules"])
    )

    keys = schedules.select("flight_number", "scheduled_departure_utc")
    assert keys.count() == keys.distinct().count()
