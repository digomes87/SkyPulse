"""Silver-shaped flight schedule and METAR rows for detector tests."""

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

SCHEDULE_SCHEMA = (
    "flight_number string, airline_icao string, origin_icao string, destination_icao string, "
    "scheduled_departure_utc timestamp, scheduled_arrival_utc timestamp, "
    "revised_departure_utc timestamp, revised_arrival_utc timestamp, "
    "runway_departure_utc timestamp, runway_arrival_utc timestamp, "
    "status string, observed_at timestamp, correlation_id string"
)
METAR_SCHEMA = (
    "station_icao string, observed_at timestamp, present_weather string, visibility_m bigint, "
    "wind_direction_degrees bigint, wind_speed_kt bigint, wind_gust_kt bigint"
)


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 14, hour, minute)  # noqa: DTZ001 - Spark session runs in UTC


def schedule(
    number: str,
    status: str,
    *,
    route: tuple[str, str] = ("EIDW", "LEMD"),
    scheduled: tuple[datetime, datetime] = (at(9), at(11)),
    revised: tuple[datetime | None, datetime | None] = (None, None),
    runway: tuple[datetime | None, datetime | None] = (None, None),
    airline: str = "RYR",
) -> tuple[object, ...]:
    return (number, airline, *route, *scheduled, *revised, *runway, status, at(20), "cid-1")


def schedules(spark: SparkSession, *rows: tuple[object, ...]) -> DataFrame:
    return spark.createDataFrame(list(rows), SCHEDULE_SCHEMA)


def metar(spark: SparkSession, *rows: tuple[object, ...]) -> DataFrame:
    return spark.createDataFrame(list(rows), METAR_SCHEMA)
