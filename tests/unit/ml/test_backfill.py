from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from skypulse.common.config import Settings
from skypulse.ml.backfill import backfill
from skypulse.ml.history import SyntheticHistory
from skypulse.streaming.bronze import bronze_path
from skypulse.streaming.silver import SILVER_TABLES, silver_path

pytestmark = pytest.mark.spark
TABLES = {table.name: table for table in SILVER_TABLES}


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("SKYPULSE_LAKE__ROOT_URI", tmp_path.as_uri())
    return Settings()


def count(spark: SparkSession, path: str) -> int:
    return spark.read.format("delta").load(path).count()


def test_history_lands_in_bronze_and_silver_and_reruns_are_idempotent(
    spark: SparkSession, settings: Settings
) -> None:
    history = SyntheticHistory(datetime(2026, 9, 1, tzinfo=UTC), days=2)
    flights, metar = history.generate()

    loaded = backfill(spark, settings, history)
    backfill(spark, settings, history)

    assert loaded == {"flight_schedules": len(flights), "metar_observations": len(metar)}
    assert count(spark, silver_path(settings, TABLES["flight_schedules"])) == len(flights)
    assert count(spark, silver_path(settings, TABLES["metar_observations"])) == len(metar)
    assert count(spark, bronze_path(settings)) == 2 * (len(flights) + len(metar))
