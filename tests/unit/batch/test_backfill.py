"""Backfill: events the silver watermark dropped are recovered from bronze, once."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from skypulse.batch.__main__ import Job, parse_arguments
from skypulse.batch.backfill import OVERLAP, bronze_window, last_cutoff, run_backfill
from skypulse.common.config import Settings
from tests.unit.streaming.events import BASE, aircraft_state
from tests.unit.streaming.test_watermarks import land_in_bronze, run_silver, silver_aircraft

pytestmark = pytest.mark.spark


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("SKYPULSE_LAKE__ROOT_URI", tmp_path.as_uri())
    return Settings()


def after_now() -> datetime:
    return datetime.now(UTC) + timedelta(minutes=1)


def drop_one_late_event(spark: SparkSession, settings: Settings) -> None:
    land_in_bronze(spark, settings, [aircraft_state("aaaaaa", BASE)])
    run_silver(spark, settings)
    land_in_bronze(spark, settings, [aircraft_state("cccccc", BASE - timedelta(minutes=30))])
    run_silver(spark, settings)


def test_events_beyond_the_watermark_are_recovered(spark: SparkSession, settings: Settings) -> None:
    drop_one_late_event(spark, settings)
    assert silver_aircraft(spark, settings) == ["aaaaaa"]

    recovered = run_backfill(spark, settings, after_now())

    assert recovered["aircraft_states"] == 1
    assert silver_aircraft(spark, settings) == ["aaaaaa", "cccccc"]


def test_rerunning_the_backfill_changes_nothing(spark: SparkSession, settings: Settings) -> None:
    drop_one_late_event(spark, settings)
    run_backfill(spark, settings, after_now())

    recovered = run_backfill(spark, settings, after_now(), full=True)

    assert recovered["aircraft_states"] == 0
    assert silver_aircraft(spark, settings) == ["aaaaaa", "cccccc"]


def test_the_cutoff_moves_to_the_run_start(spark: SparkSession, settings: Settings) -> None:
    drop_one_late_event(spark, settings)
    started = after_now().replace(microsecond=0)

    run_backfill(spark, settings, started)

    assert last_cutoff(spark, settings) == started


def test_the_window_starts_one_overlap_before_the_last_cutoff(spark: SparkSession) -> None:
    cutoff = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
    rows = [
        (cutoff - OVERLAP - timedelta(minutes=1), "too old"),
        (cutoff - OVERLAP, "overlap start"),
        (cutoff + timedelta(hours=1), "new"),
        (cutoff + timedelta(hours=3), "after the run started"),
    ]
    bronze = spark.createDataFrame(
        [(at, at.date(), label) for at, label in rows],
        "ingested_at timestamp, ingest_date date, label string",
    )

    window = bronze_window(bronze, cutoff, cutoff + timedelta(hours=2))

    assert sorted(row["label"] for row in window.collect()) == ["new", "overlap start"]


def test_backfill_is_a_batch_job() -> None:
    arguments = parse_arguments(["backfill", "--full"])

    assert arguments.job is Job.BACKFILL
    assert arguments.full
