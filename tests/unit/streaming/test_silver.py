from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from skypulse.streaming.bronze import to_bronze
from skypulse.streaming.silver import (
    SILVER_TABLES,
    MergePolicy,
    latest_per_key,
    merge_into,
    parse,
    quarantined_rows,
    valid_rows,
)
from tests.unit.streaming.events import BASE, metar
from tests.unit.streaming.kafka_rows import kafka_frame

pytestmark = pytest.mark.spark

METAR_TABLE = next(table for table in SILVER_TABLES if table.name == "metar_observations")


def bronze_of(
    spark: SparkSession, values: list[str], topic: str = "skypulse.raw.metar"
) -> DataFrame:
    return to_bronze(kafka_frame(spark, topic, values, headers={"correlation-id": "cid-1"}))


def test_valid_events_are_flattened_with_typed_columns(spark: SparkSession) -> None:
    parsed = parse(bronze_of(spark, [metar("EIDW", 0, gust=26)]), METAR_TABLE)

    row = valid_rows(parsed, METAR_TABLE).first()

    assert row is not None
    assert row["station_icao"] == "EIDW"
    assert row["wind_gust_kt"] == 26
    assert row["observed_at"] == BASE.replace(tzinfo=None)
    assert row["event_time"] == row["observed_at"]
    assert row["correlation_id"] == "cid-1"


def test_bad_rows_are_quarantined_with_reasons(spark: SparkSession) -> None:
    broken = metar("EIDW", 0).replace('"station_icao":"EIDW",', "")
    bronze = bronze_of(spark, ["{not json", broken, metar("EGLL", 0)])
    parsed = parse(bronze, METAR_TABLE)

    reasons = sorted(row["reason"] for row in quarantined_rows(parsed, METAR_TABLE).collect())

    assert valid_rows(parsed, METAR_TABLE).count() == 1
    assert reasons[0] == "missing data.station_icao"
    assert reasons[1].startswith("payload is not valid JSON")


def test_other_topics_are_ignored(spark: SparkSession) -> None:
    bronze = bronze_of(spark, [metar("EIDW", 0)], topic="skypulse.raw.aircraft-states")

    assert parse(bronze, METAR_TABLE).count() == 0


SCHEDULE_COLUMNS = (
    "flight_number string, scheduled string, status string, "
    "observed_at int, ingested_at int, event_id string"
)
SCHEDULE_POLICY = MergePolicy(("flight_number", "scheduled"), newer_by="observed_at")


def schedules(spark: SparkSession, rows: list[tuple[object, ...]]) -> DataFrame:
    return spark.createDataFrame(rows, SCHEDULE_COLUMNS)


def statuses(frame: DataFrame) -> dict[str, str]:
    return {row["flight_number"]: row["status"] for row in frame.collect()}


def test_latest_per_key_keeps_the_newest_version(spark: SparkSession) -> None:
    rows = schedules(
        spark,
        [
            ("FR1", "06:55", "Expected", 1, 10, "a"),
            ("FR1", "06:55", "Departed", 3, 11, "b"),
            ("FR2", "07:00", "Expected", 2, 12, "c"),
        ],
    )

    assert statuses(latest_per_key(rows, SCHEDULE_POLICY)) == {"FR1": "Departed", "FR2": "Expected"}


def test_merge_updates_only_with_newer_versions(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "flight_schedules")

    merge_into(
        spark, schedules(spark, [("FR1", "06:55", "Boarding", 5, 1, "a")]), path, SCHEDULE_POLICY
    )
    merge_into(
        spark, schedules(spark, [("FR1", "06:55", "Expected", 2, 2, "b")]), path, SCHEDULE_POLICY
    )
    merge_into(
        spark, schedules(spark, [("FR1", "06:55", "Departed", 9, 3, "c")]), path, SCHEDULE_POLICY
    )

    assert statuses(spark.read.format("delta").load(path)) == {"FR1": "Departed"}


def test_insert_only_merge_is_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "metar")
    policy = MergePolicy(("flight_number", "scheduled"))
    batch = schedules(
        spark, [("FR1", "06:55", "Expected", 1, 1, "a"), ("FR2", "07:00", "Expected", 1, 1, "b")]
    )

    merge_into(spark, batch, path, policy)
    merge_into(spark, batch, path, policy)

    assert spark.read.format("delta").load(path).count() == 2
