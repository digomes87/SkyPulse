"""Late data: inside the watermark it reaches silver once; beyond it, it stays in bronze only."""

from datetime import timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from skypulse.common.config import Settings
from skypulse.streaming.bronze import bronze_path, to_bronze
from skypulse.streaming.jobs import start_quarantine, start_silver
from skypulse.streaming.silver import SILVER_TABLES, quarantine_path, silver_path
from skypulse.streaming.summary import table_counts
from skypulse.streaming.triggers import Trigger
from tests.unit.streaming.events import BASE, aircraft_state
from tests.unit.streaming.kafka_rows import kafka_frame

pytestmark = pytest.mark.spark

STATES = next(table for table in SILVER_TABLES if table.name == "aircraft_states")
TOPIC = "skypulse.raw.aircraft-states"


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("SKYPULSE_LAKE__ROOT_URI", tmp_path.as_uri())
    return Settings()


def land_in_bronze(spark: SparkSession, settings: Settings, values: list[str]) -> None:
    frame = to_bronze(kafka_frame(spark, TOPIC, values))
    frame.write.format("delta").mode("append").save(bronze_path(settings))


def run_silver(spark: SparkSession, settings: Settings) -> None:
    start_silver(spark, settings, STATES, Trigger(available_now=True)).awaitTermination()


def silver_aircraft(spark: SparkSession, settings: Settings) -> list[str]:
    rows = spark.read.format("delta").load(silver_path(settings, STATES)).collect()
    return sorted(row["icao24"] for row in rows)


def test_late_events_within_the_watermark_are_kept_once(
    spark: SparkSession, settings: Settings
) -> None:
    on_time = aircraft_state("aaaaaa", BASE)
    land_in_bronze(spark, settings, [on_time])
    run_silver(spark, settings)

    slightly_late = aircraft_state("bbbbbb", BASE - timedelta(minutes=5))
    land_in_bronze(spark, settings, [slightly_late, on_time])
    run_silver(spark, settings)

    assert silver_aircraft(spark, settings) == ["aaaaaa", "bbbbbb"]


def test_events_beyond_the_watermark_stay_in_bronze_only(
    spark: SparkSession, settings: Settings
) -> None:
    land_in_bronze(spark, settings, [aircraft_state("aaaaaa", BASE)])
    run_silver(spark, settings)

    too_late = aircraft_state("cccccc", BASE - timedelta(minutes=30))
    land_in_bronze(spark, settings, [too_late])
    run_silver(spark, settings)

    bronze_rows = spark.read.format("delta").load(bronze_path(settings)).count()
    assert silver_aircraft(spark, settings) == ["aaaaaa"]
    assert bronze_rows == 2


def test_replaying_bronze_does_not_duplicate_silver(
    spark: SparkSession, settings: Settings
) -> None:
    event = aircraft_state("aaaaaa", BASE)
    land_in_bronze(spark, settings, [event, event])
    run_silver(spark, settings)
    land_in_bronze(spark, settings, [event])
    run_silver(spark, settings)

    assert silver_aircraft(spark, settings) == ["aaaaaa"]


def test_invalid_rows_land_in_quarantine(spark: SparkSession, settings: Settings) -> None:
    land_in_bronze(spark, settings, ["{broken", aircraft_state("aaaaaa", BASE)])

    start_quarantine(spark, settings, STATES, Trigger(available_now=True)).awaitTermination()

    rows = spark.read.format("delta").load(quarantine_path(settings)).collect()
    assert [row["silver_table"] for row in rows] == ["aircraft_states"]


def test_summary_counts_each_table(spark: SparkSession, settings: Settings) -> None:
    land_in_bronze(spark, settings, [aircraft_state("aaaaaa", BASE)])
    run_silver(spark, settings)

    counts = table_counts(spark, settings)

    assert counts["bronze.raw_events"] == 1
    assert counts["silver.aircraft_states"] == 1
    assert counts["silver.metar_observations"] == 0
