"""Reconciliation: every bronze record is accounted for downstream."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from pyspark.sql import DataFrame, SparkSession

from skypulse.batch.backfill import run_backfill
from skypulse.common.config import LakeLayer, Settings
from skypulse.quality.jobs import check_reconciliation
from skypulse.quality.reconciliation import (
    GOLD_FACTS,
    bronze_to_quarantine,
    bronze_to_silver,
    recent,
    silver_to_gold,
)
from skypulse.quality.results import Status
from skypulse.streaming.bronze import bronze_path
from skypulse.streaming.jobs import ensure_quarantine_table, start_quarantine
from skypulse.streaming.silver import quarantine_path, silver_path
from skypulse.streaming.triggers import Trigger
from tests.unit.streaming.events import BASE, aircraft_state
from tests.unit.streaming.schedule_rows import schedule, schedules
from tests.unit.streaming.test_watermarks import STATES, land_in_bronze, run_silver

pytestmark = pytest.mark.spark


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("SKYPULSE_LAKE__ROOT_URI", tmp_path.as_uri())
    return Settings()


def now() -> datetime:
    return datetime.now(UTC) + timedelta(minutes=1)


def read(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def bronze(spark: SparkSession, settings: Settings) -> DataFrame:
    return recent(read(spark, bronze_path(settings)), now())


def silver(spark: SparkSession, settings: Settings) -> DataFrame:
    return read(spark, silver_path(settings, STATES))


def test_a_late_event_shows_up_as_missing_until_the_backfill(
    spark: SparkSession, settings: Settings
) -> None:
    land_in_bronze(spark, settings, [aircraft_state("aaaaaa", BASE)])
    run_silver(spark, settings)
    land_in_bronze(spark, settings, [aircraft_state("cccccc", BASE - timedelta(minutes=30))])
    run_silver(spark, settings)

    before = bronze_to_silver(bronze(spark, settings), silver(spark, settings), STATES)
    run_backfill(spark, settings, now())
    after = bronze_to_silver(bronze(spark, settings), silver(spark, settings), STATES)

    assert (before.status, before.observed, before.details) == (Status.FAIL, 1, "1 of 2 missing")
    assert (after.status, after.observed) == (Status.PASS, 0)


def test_a_missing_silver_table_misses_every_event(spark: SparkSession, settings: Settings) -> None:
    land_in_bronze(spark, settings, [aircraft_state("aaaaaa", BASE)])

    result = bronze_to_silver(bronze(spark, settings), None, STATES)

    assert (result.status, result.observed) == (Status.FAIL, 1)


def test_invalid_records_must_reach_the_quarantine(spark: SparkSession, settings: Settings) -> None:
    land_in_bronze(spark, settings, ["not json", aircraft_state("aaaaaa", BASE)])

    unprocessed = bronze_to_quarantine(bronze(spark, settings), None, STATES)
    ensure_quarantine_table(spark, settings, STATES)
    start_quarantine(spark, settings, STATES, Trigger(available_now=True)).awaitTermination()
    quarantine = read(spark, quarantine_path(settings))
    processed = bronze_to_quarantine(bronze(spark, settings), quarantine, STATES)

    assert (unprocessed.status, unprocessed.observed) == (Status.FAIL, 1)
    assert (processed.status, processed.details) == (Status.PASS, "0 of 1 missing")


def test_gold_facts_keep_one_row_per_silver_row() -> None:
    fact = GOLD_FACTS[0]

    assert silver_to_gold(fact, 10, 10).status is Status.PASS
    mismatch = silver_to_gold(fact, 10, 7)
    assert (mismatch.status, mismatch.observed) == (Status.FAIL, 3)
    assert mismatch.details == "silver.flight_schedules=10, gold.fct_flight=7"


def gold_database(tmp_path: Path, flights: int) -> Path:
    database = tmp_path / "gold.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("create schema main_gold")
        connection.execute(
            f"create table main_gold.fct_flight as select i from range({flights}) t(i)"
        )
        connection.execute("create table main_gold.fct_eu261_claim (claim_id varchar)")
    return database


def test_reconciliation_stage_reports_every_check(
    spark: SparkSession, settings: Settings, tmp_path: Path
) -> None:
    path = f"{settings.lake.uri_for(LakeLayer.SILVER)}/flight_schedules"
    schedules(spark, schedule("FR1", "Arrived")).write.format("delta").save(path)

    report = check_reconciliation(spark, settings, gold_database(tmp_path, 2), now())

    by_check = {(result.suite, result.check): result.status for result in report}
    assert by_check == {
        ("reconciliation", "bronze"): Status.SKIP,
        ("reconciliation.silver_to_gold", "fct_flight"): Status.FAIL,
        ("reconciliation.silver_to_gold", "fct_eu261_claim"): Status.PASS,
    }
