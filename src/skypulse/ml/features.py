"""Offline features: one row per flight, as known at prediction time.

Point-in-time rules (see ``feature_spec``):
- prediction time is two hours before the scheduled departure;
- weather is the latest METAR at each airport reported before prediction time (and no
  older than three hours);
- route and airline statistics only count flights that had landed before prediction time,
  over the previous 30 days.

Nothing that happens after prediction time, including the flight's own outcome, can leak
into its features. Labels (delay at least 15 and 180 minutes) are kept beside them, only
for landed flights; cancellations are a different problem and get no label.
"""

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window, functions

from skypulse.common.config import LakeLayer, Settings
from skypulse.ml.feature_spec import (
    FEATURE_SET_VERSION,
    FEATURES,
    HISTORY_WINDOW_DAYS,
    KEY_COLUMNS,
    LABELS,
    PRECIPITATION_PATTERN,
    PREDICTION_LEAD,
    THUNDERSTORM_PATTERN,
    WEATHER_LOOKBACK,
)
from skypulse.streaming.disruptions import reference_airports

FEATURES_TABLE = "flight_features"
STATS_TABLE = "flight_feature_stats"
EARTH_RADIUS_KM = 6371.0
SECONDS_PER_DAY = 86_400


def features_path(settings: Settings, table: str = FEATURES_TABLE) -> str:
    return f"{settings.lake.uri_for(LakeLayer.GOLD)}/{table}"


def _epoch(column: str | Column) -> Column:
    return functions.unix_timestamp(column)


def flights(schedules: DataFrame) -> DataFrame:
    landed = functions.col("runway_arrival_utc").isNotNull() | (
        functions.col("status") == "Arrived"
    )
    actual_arrival = functions.when(
        landed, functions.coalesce("runway_arrival_utc", "revised_arrival_utc")
    )
    delay = ((_epoch("actual_arrival_utc") - _epoch("scheduled_arrival_utc")) / 60).cast("int")
    with_outcome = (
        schedules.select(
            *KEY_COLUMNS,
            "airline_icao",
            "origin_icao",
            "destination_icao",
            "scheduled_arrival_utc",
            "status",
            "revised_arrival_utc",
            "runway_arrival_utc",
        )
        .withColumn("actual_arrival_utc", actual_arrival)
        .withColumn("arrival_delay_minutes", delay)
        .withColumn(
            "prediction_time",
            functions.col("scheduled_departure_utc")
            - functions.expr(f"INTERVAL {int(PREDICTION_LEAD.total_seconds())} SECONDS"),
        )
    )
    labels = [
        functions.when(
            functions.col("actual_arrival_utc").isNotNull(),
            (functions.col("arrival_delay_minutes") >= minutes).cast("int"),
        ).alias(name)
        for name, minutes in LABELS.items()
    ]
    return with_outcome.select("*", *labels)


def _latest_weather(
    flights_frame: DataFrame, metar: DataFrame, station: str, prefix: str
) -> DataFrame:
    observations = metar.select(
        functions.col("station_icao").alias("_station"),
        functions.col("observed_at").alias("_observed_at"),
        functions.col("visibility_m").alias(f"{prefix}_visibility_m"),
        functions.col("wind_speed_kt").alias(f"{prefix}_wind_kt"),
        functions.coalesce("wind_gust_kt", functions.lit(0)).alias(f"{prefix}_gust_kt"),
        functions.coalesce(
            functions.col("present_weather").rlike(THUNDERSTORM_PATTERN), functions.lit(False)
        )
        .cast("int")
        .alias(f"{prefix}_thunderstorm"),
        functions.coalesce(
            functions.col("present_weather").rlike(PRECIPITATION_PATTERN), functions.lit(False)
        )
        .cast("int")
        .alias(f"{prefix}_precipitation"),
    )
    before_prediction = (
        (functions.col("_station") == functions.col(station))
        & (functions.col("_observed_at") <= functions.col("prediction_time"))
        & (
            _epoch("_observed_at")
            > _epoch("prediction_time") - int(WEATHER_LOOKBACK.total_seconds())
        )
    )
    latest = Window.partitionBy(*KEY_COLUMNS).orderBy(functions.col("_observed_at").desc())
    joined = flights_frame.select(*KEY_COLUMNS, station, "prediction_time").join(
        observations, before_prediction, "inner"
    )
    newest = joined.withColumn("_rank", functions.row_number().over(latest)).where("_rank = 1")
    return newest.drop("_rank", "_station", "_observed_at", station, "prediction_time")


def _history(flights_frame: DataFrame, group: tuple[str, ...], prefix: str) -> DataFrame:
    landed = flights_frame.where(functions.col("actual_arrival_utc").isNotNull()).select(
        *(functions.col(column).alias(f"_{column}") for column in group),
        functions.col("actual_arrival_utc").alias("_arrived"),
        functions.col("arrival_delay_minutes").alias("_delay"),
    )
    target = flights_frame.select(*KEY_COLUMNS, *group, "prediction_time")
    window_start = _epoch("prediction_time") - HISTORY_WINDOW_DAYS * SECONDS_PER_DAY
    condition = functions.lit(True)
    for column in group:
        condition = condition & (functions.col(column) == functions.col(f"_{column}"))
    condition = (
        condition
        & (functions.col("_arrived") < functions.col("prediction_time"))
        & (_epoch("_arrived") >= window_start)
    )
    return (
        target.join(landed, condition, "inner")
        .groupBy(*KEY_COLUMNS)
        .agg(
            functions.avg("_delay").alias(f"{prefix}_mean_arrival_delay"),
            functions.avg((functions.col("_delay") >= LABELS["delayed_15"]).cast("double")).alias(
                f"{prefix}_delay_15_rate"
            ),
            functions.count("*").alias(f"{prefix}_flights_30d"),
        )
    )


def _haversine(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    half_lat = functions.radians(lat2 - lat1) / 2
    half_lon = functions.radians(lon2 - lon1) / 2
    inner = functions.pow(functions.sin(half_lat), 2) + functions.cos(
        functions.radians(lat1)
    ) * functions.cos(functions.radians(lat2)) * functions.pow(functions.sin(half_lon), 2)
    return 2 * EARTH_RADIUS_KM * functions.asin(functions.sqrt(inner))


def _distances(flights_frame: DataFrame, airports: DataFrame) -> DataFrame:
    origin = airports.select(
        functions.col("icao").alias("origin_icao"),
        functions.col("latitude").alias("_lat1"),
        functions.col("longitude").alias("_lon1"),
    )
    destination = airports.select(
        functions.col("icao").alias("destination_icao"),
        functions.col("latitude").alias("_lat2"),
        functions.col("longitude").alias("_lon2"),
    )
    routes = flights_frame.select("origin_icao", "destination_icao").distinct()
    measured = routes.join(origin, "origin_icao", "left").join(
        destination, "destination_icao", "left"
    )
    distance = _haversine(
        functions.col("_lat1"),
        functions.col("_lon1"),
        functions.col("_lat2"),
        functions.col("_lon2"),
    )
    return measured.select("origin_icao", "destination_icao", distance.alias("distance_km"))


def build_features(schedules: DataFrame, metar: DataFrame, airports: DataFrame) -> DataFrame:
    base = flights(schedules)
    calendar = base.withColumn(
        "departure_hour_utc", functions.hour("scheduled_departure_utc")
    ).withColumn("day_of_week", (functions.dayofweek("scheduled_departure_utc") + 5) % 7)
    parts = [
        _latest_weather(base, metar, "origin_icao", "origin"),
        _latest_weather(base, metar, "destination_icao", "destination").drop(
            "destination_gust_kt", "destination_precipitation"
        ),
        _history(base, ("origin_icao", "destination_icao"), "route").drop(
            "route_mean_arrival_delay_unused"
        ),
        _history(base, ("airline_icao",), "airline").select(*KEY_COLUMNS, "airline_delay_15_rate"),
    ]
    joined = calendar
    for part in parts:
        joined = joined.join(part, list(KEY_COLUMNS), "left")
    joined = joined.join(_distances(base, airports), ["origin_icao", "destination_icao"], "left")
    return joined.select(
        *KEY_COLUMNS,
        "prediction_time",
        *FEATURES,
        "arrival_delay_minutes",
        *LABELS,
        functions.lit(FEATURE_SET_VERSION).alias("feature_set_version"),
    )


def latest_stats(features: DataFrame) -> DataFrame:
    """The newest route and airline statistics, for online serving."""
    window = Window.partitionBy("origin_icao", "destination_icao", "airline_icao").orderBy(
        functions.col("prediction_time").desc()
    )
    return (
        features.withColumn("_rank", functions.row_number().over(window))
        .where("_rank = 1")
        .select(
            "origin_icao",
            "destination_icao",
            "airline_icao",
            "route_mean_arrival_delay",
            "route_delay_15_rate",
            "route_flights_30d",
            "airline_delay_15_rate",
            "prediction_time",
        )
    )


def build_feature_tables(spark: SparkSession, settings: Settings) -> int:
    silver = settings.lake.uri_for(LakeLayer.SILVER)
    schedules = spark.read.format("delta").load(f"{silver}/flight_schedules")
    metar = spark.read.format("delta").load(f"{silver}/metar_observations")
    reference = f"{silver}/reference_airports"
    loaded = spark.read.format("delta").load(reference) if _exists(spark, reference) else None
    features = build_features(schedules, metar, reference_airports(spark, loaded)).cache()
    features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        features_path(settings)
    )
    latest_stats(features).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(features_path(settings, STATS_TABLE))
    count = features.count()
    features.unpersist()
    return count


def _exists(spark: SparkSession, path: str) -> bool:
    return bool(DeltaTable.isDeltaTable(spark, path))
