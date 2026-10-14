"""Turns flight schedule changes into flight_disruption events.

Classification, per changed flight (first match wins):

1. ``cancellation``: status Canceled or CanceledUncertain.
2. ``arrival_delay``: the flight has landed (runway arrival known, or status Arrived) at
   least 15 minutes after its scheduled arrival. Final outcome.
3. ``departure_delay``: not landed yet, departure at least 60 minutes late. Early warning.

``detected_at`` is the time the schedule provider was observed, not the wall clock, so
replays produce the same events as the original run.
"""

from pyspark.sql import Column, DataFrame, SparkSession, functions

from skypulse.reference.registry import airlines, airports

NOTABLE_ARRIVAL_DELAY_MINUTES = 15
NOTABLE_DEPARTURE_DELAY_MINUTES = 60
WEATHER_WINDOW_SECONDS = 3600
CANCELLED_STATUSES = ("Canceled", "CanceledUncertain")
NATURAL_KEY = ("flight_number", "scheduled_departure_utc", "kind")
WEATHER_FIELDS = (
    "observed_at",
    "present_weather",
    "visibility_m",
    "wind_direction_degrees",
    "wind_speed_kt",
    "wind_gust_kt",
)


def _minutes_between(later: str | Column, earlier: str | Column) -> Column:
    seconds = functions.unix_timestamp(later) - functions.unix_timestamp(earlier)
    return (seconds / 60).cast("int")


def classify(schedules: DataFrame) -> DataFrame:
    """Schedules with ``kind`` and delay columns; flights that are not disrupted are dropped."""
    actual_departure = functions.coalesce("runway_departure_utc", "revised_departure_utc")
    landed = functions.col("runway_arrival_utc").isNotNull() | (
        functions.col("status") == "Arrived"
    )
    actual_arrival = functions.when(
        landed, functions.coalesce("runway_arrival_utc", "revised_arrival_utc")
    )
    with_delays = (
        schedules.withColumn("actual_departure_utc", actual_departure)
        .withColumn("actual_arrival_utc", actual_arrival)
        .withColumn(
            "departure_delay_minutes",
            _minutes_between("actual_departure_utc", "scheduled_departure_utc"),
        )
        .withColumn(
            "arrival_delay_minutes",
            _minutes_between("actual_arrival_utc", "scheduled_arrival_utc"),
        )
    )
    kind = (
        functions.when(functions.col("status").isin(*CANCELLED_STATUSES), "cancellation")
        .when(
            functions.col("arrival_delay_minutes") >= NOTABLE_ARRIVAL_DELAY_MINUTES,
            "arrival_delay",
        )
        .when(
            functions.col("actual_arrival_utc").isNull()
            & (functions.col("departure_delay_minutes") >= NOTABLE_DEPARTURE_DELAY_MINUTES),
            "departure_delay",
        )
    )
    return with_delays.withColumn("kind", kind).where(functions.col("kind").isNotNull())


def reference_airports(spark: SparkSession) -> DataFrame:
    rows = [(row["icao"], row["country"], row["latitude"], row["longitude"]) for row in airports()]
    return spark.createDataFrame(
        rows, "icao string, country string, latitude double, longitude double"
    )


def reference_airlines(spark: SparkSession) -> DataFrame:
    rows = [(row["icao"], row["country"]) for row in airlines()]
    return spark.createDataFrame(rows, "airline_icao string, airline_country string")


def _airport_struct(code_column: str, reference: DataFrame, alias: str) -> tuple[DataFrame, Column]:
    renamed = reference.select(
        functions.col("icao").alias(f"{alias}_icao_ref"),
        functions.struct(functions.col("icao"), "country", "latitude", "longitude").alias(
            f"{alias}_reference"
        ),
    )
    fallback = functions.struct(
        functions.col(code_column).alias("icao"),
        functions.lit(None).cast("string").alias("country"),
        functions.lit(None).cast("double").alias("latitude"),
        functions.lit(None).cast("double").alias("longitude"),
    )
    value = functions.coalesce(functions.col(f"{alias}_reference"), fallback)
    return renamed, value


def _weather_near(
    disruptions: DataFrame, metar: DataFrame, station_column: str, moment_column: str, alias: str
) -> DataFrame:
    observations = metar.select(
        functions.col("station_icao").alias("_station"),
        functions.struct(*WEATHER_FIELDS).alias("_observation"),
        functions.col("observed_at").alias("_observed_at"),
    )
    near = (functions.col("_station") == functions.col(station_column)) & (
        functions.abs(
            functions.unix_timestamp("_observed_at") - functions.unix_timestamp(moment_column)
        )
        <= WEATHER_WINDOW_SECONDS
    )
    matched = functions.when(functions.col("_station").isNotNull(), functions.col("_observation"))
    grouped = (
        disruptions.select(*NATURAL_KEY, station_column, moment_column)
        .join(observations, near, "left")
        .groupBy(*NATURAL_KEY)
        .agg(functions.sort_array(functions.collect_list(matched)).alias(alias))
    )
    return disruptions.join(grouped, list(NATURAL_KEY), "left")


def enrich(classified: DataFrame, metar: DataFrame) -> DataFrame:
    """Adds airport and airline reference data and METAR near departure and arrival."""
    spark = classified.sparkSession
    reference = reference_airports(spark)
    origins, origin_value = _airport_struct("origin_icao", reference, "origin")
    destinations, destination_value = _airport_struct("destination_icao", reference, "destination")
    with_reference = (
        classified.join(
            origins, functions.col("origin_icao") == functions.col("origin_icao_ref"), "left"
        )
        .join(
            destinations,
            functions.col("destination_icao") == functions.col("destination_icao_ref"),
            "left",
        )
        .join(reference_airlines(spark), "airline_icao", "left")
        .withColumn("origin", origin_value)
        .withColumn("destination", destination_value)
    )
    with_origin_weather = _weather_near(
        with_reference, metar, "origin_icao", "scheduled_departure_utc", "origin_weather"
    )
    return _weather_near(
        with_origin_weather,
        metar,
        "destination_icao",
        "scheduled_arrival_utc",
        "destination_weather",
    ).withColumn("detected_at", functions.col("observed_at"))


def not_yet_announced(candidates: DataFrame, announced: DataFrame | None) -> DataFrame:
    """Drops disruptions whose (flight, scheduled departure, kind) was already published."""
    unique = candidates.dropDuplicates(list(NATURAL_KEY))
    if announced is None:
        return unique
    return unique.join(announced.select(*NATURAL_KEY), list(NATURAL_KEY), "left_anti")


EVENT_FIELDS = (
    "flight_number",
    "airline_icao",
    "airline_country",
    "origin",
    "destination",
    "scheduled_departure_utc",
    "scheduled_arrival_utc",
    "actual_departure_utc",
    "actual_arrival_utc",
    "kind",
    "status",
    "departure_delay_minutes",
    "arrival_delay_minutes",
    "detected_at",
    "origin_weather",
    "destination_weather",
)
JSON_OPTIONS = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss'Z'", "ignoreNullFields": "false"}


def as_payloads(enriched: DataFrame) -> DataFrame:
    """The contract's ``data`` object as JSON text, rendered by Spark in UTC."""
    payload = functions.to_json(functions.struct(*EVENT_FIELDS), JSON_OPTIONS)
    return enriched.select(*NATURAL_KEY, "correlation_id", payload.alias("payload"))
