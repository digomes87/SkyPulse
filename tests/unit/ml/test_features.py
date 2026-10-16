"""The feature pipeline must never see the future relative to prediction time."""

from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession

from skypulse.ml.feature_spec import FEATURES, LABELS
from skypulse.ml.features import build_features, latest_stats
from skypulse.streaming.disruptions import reference_airports
from tests.unit.streaming.schedule_rows import METAR_SCHEMA, SCHEDULE_SCHEMA

pytestmark = pytest.mark.spark


def at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, day, hour, minute)  # noqa: DTZ001 - Spark session runs in UTC


def flight(
    number: str, departure: datetime, arrival: datetime, landed: datetime | None
) -> tuple[object, ...]:
    status = "Arrived" if landed else "Canceled"
    return (
        number,
        "RYR",
        "EIDW",
        "LEMD",
        departure,
        arrival,
        None,
        None,
        None,
        landed,
        status,
        at(20, 0),
        "c",
    )


def build(
    spark: SparkSession, flights: list[tuple[object, ...]], metar: list[tuple[object, ...]]
) -> dict[str, dict[str, object]]:
    schedules = spark.createDataFrame(flights, SCHEDULE_SCHEMA)
    observations = (
        spark.createDataFrame(metar, METAR_SCHEMA)
        if metar
        else spark.createDataFrame([], METAR_SCHEMA)
    )
    rows = build_features(schedules, observations, reference_airports(spark)).collect()
    return {row["flight_number"]: row.asDict() for row in rows}


def test_output_has_every_feature_and_label(spark: SparkSession) -> None:
    rows = build(spark, [flight("FR1", at(10, 9), at(10, 11), at(10, 11, 30))], [])

    assert set(FEATURES) | set(LABELS) <= rows["FR1"].keys()
    assert rows["FR1"]["delayed_15"] == 1
    assert rows["FR1"]["delayed_180"] == 0
    assert rows["FR1"]["distance_km"] == pytest.approx(1452, abs=10)


def test_weather_is_the_latest_report_before_prediction_time(spark: SparkSession) -> None:
    metar: list[tuple[object, ...]] = [
        ("EIDW", at(10, 6, 0), None, 9999, 200, 10, None),  # prediction time is 07:00
        ("EIDW", at(10, 6, 50), "+TSRA", 1500, 240, 25, 40),  # latest before 07:00
        ("EIDW", at(10, 8, 0), "FG", 200, 0, 2, None),  # after prediction time: must be ignored
    ]

    rows = build(spark, [flight("FR1", at(10, 9), at(10, 11), at(10, 11))], metar)

    assert rows["FR1"]["origin_thunderstorm"] == 1
    assert rows["FR1"]["origin_visibility_m"] == 1500


def test_route_history_only_counts_flights_landed_before_prediction_time(
    spark: SparkSession,
) -> None:
    flights = [
        flight("FR1", at(8, 9), at(8, 11), at(8, 14)),  # landed two days earlier, 180 min late
        flight(
            "FR2", at(10, 6), at(10, 8), at(10, 8)
        ),  # landed 08:00, after FR3's prediction time (07:00)
        flight("FR3", at(10, 9), at(10, 11), at(10, 11)),
    ]

    rows = build(spark, flights, [])

    assert rows["FR3"]["route_flights_30d"] == 1
    assert rows["FR3"]["route_mean_arrival_delay"] == 180
    assert rows["FR1"]["route_flights_30d"] is None


def test_a_flight_never_counts_in_its_own_history(spark: SparkSession) -> None:
    rows = build(spark, [flight("FR1", at(10, 9), at(10, 11), at(10, 13))], [])

    assert rows["FR1"]["route_flights_30d"] is None
    assert rows["FR1"]["airline_delay_15_rate"] is None


def test_cancelled_flights_get_no_label(spark: SparkSession) -> None:
    rows = build(spark, [flight("FR1", at(10, 9), at(10, 11), None)], [])

    assert rows["FR1"]["delayed_15"] is None


def test_latest_stats_keep_one_row_per_route_and_airline(spark: SparkSession) -> None:
    flights = [flight(f"FR{n}", at(10 + n, 9), at(10 + n, 11), at(10 + n, 11)) for n in range(4)]
    schedules = spark.createDataFrame(flights, SCHEDULE_SCHEMA)
    features: DataFrame = build_features(
        schedules, spark.createDataFrame([], METAR_SCHEMA), reference_airports(spark)
    )

    stats = latest_stats(features).collect()

    assert len(stats) == 1
    assert stats[0]["route_flights_30d"] == 3
