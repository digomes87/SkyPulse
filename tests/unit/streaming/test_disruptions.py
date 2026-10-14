import pytest
from pyspark.sql import SparkSession

from skypulse.streaming.disruptions import classify, enrich, not_yet_announced
from tests.unit.streaming.schedule_rows import at, metar, schedule, schedules

pytestmark = pytest.mark.spark


def kinds(spark: SparkSession, *rows: tuple[object, ...]) -> dict[str, str]:
    return {
        row["flight_number"]: row["kind"] for row in classify(schedules(spark, *rows)).collect()
    }


def test_classification(spark: SparkSession) -> None:
    result = kinds(
        spark,
        schedule("FR1", "Canceled"),
        schedule("FR2", "Arrived", revised=(at(12), at(14)), runway=(at(12), at(14, 10))),
        schedule("FR3", "Delayed", revised=(at(10, 30), at(12, 30))),
        schedule("FR4", "Arrived", runway=(at(9, 5), at(11, 10))),
        schedule("FR5", "Expected"),
    )

    assert result == {"FR1": "cancellation", "FR2": "arrival_delay", "FR3": "departure_delay"}


def test_arrival_delay_is_measured_on_the_runway_time(spark: SparkSession) -> None:
    row = classify(
        schedules(spark, schedule("FR2", "Arrived", runway=(at(12), at(14, 10))))
    ).first()

    assert row is not None
    assert row["arrival_delay_minutes"] == 190
    assert row["departure_delay_minutes"] == 180


def test_enrichment_adds_reference_data_and_nearby_weather(spark: SparkSession) -> None:
    classified = classify(schedules(spark, schedule("FR6403", "Canceled", route=("LEPA", "EGSS"))))
    weather = metar(
        spark,
        ("LEPA", at(8, 20), "+TSRA", 1200, 240, 28, 45),
        ("LEPA", at(5, 0), None, 9999, 200, 5, None),
        ("EGSS", at(11, 0), None, 9999, 250, 12, None),
    )

    row = enrich(classified, weather).first()

    assert row is not None
    assert row["origin"]["country"] == "ES"
    assert row["airline_country"] == "IE"
    assert [item["present_weather"] for item in row["origin_weather"]] == ["+TSRA"]
    assert len(row["destination_weather"]) == 1
    assert row["detected_at"] == row["observed_at"]


def test_unknown_airports_keep_their_code_without_coordinates(spark: SparkSession) -> None:
    classified = classify(schedules(spark, schedule("XX1", "Canceled", route=("EIDW", "ZZZZ"))))

    row = enrich(classified, metar(spark)).first()

    assert row is not None
    assert row["destination"]["icao"] == "ZZZZ"
    assert row["destination"]["latitude"] is None
    assert row["destination_weather"] == []


def test_only_new_disruptions_are_announced(spark: SparkSession) -> None:
    candidates = classify(
        schedules(
            spark,
            schedule("FR1", "Canceled"),
            schedule("FR1", "Canceled"),
            schedule("FR9", "Canceled"),
        )
    )
    announced = classify(schedules(spark, schedule("FR1", "Canceled")))

    fresh = not_yet_announced(candidates, announced)

    assert [row["flight_number"] for row in fresh.collect()] == ["FR9"]
    assert not_yet_announced(candidates, None).count() == 2
