"""Streaming job: silver flight_schedules change feed -> flight_disruption announcements.

Per micro-batch: classify the changed flights, enrich them, drop what was already
announced, publish the rest and record them in ``silver/flight_disruptions``. If the job
dies between publishing and recording, the retry publishes again: delivery is
at-least-once and the calculator's conditional write absorbs the duplicate.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions
from pyspark.sql.streaming.query import StreamingQuery

from skypulse.common.config import LakeLayer, Settings
from skypulse.common.logging import get_logger
from skypulse.streaming.announcements import Announcement, Announcer
from skypulse.streaming.bronze import checkpoint_path
from skypulse.streaming.disruptions import (
    NATURAL_KEY,
    as_payloads,
    classify,
    enrich,
    not_yet_announced,
)
from skypulse.streaming.silver import SILVER_TABLES, silver_path
from skypulse.streaming.triggers import Trigger

DISRUPTIONS_TABLE = "flight_disruptions"
CURRENT_VERSIONS = ("insert", "update_postimage")
METAR_SCHEMA = (
    "station_icao string, observed_at timestamp, present_weather string, visibility_m bigint, "
    "wind_direction_degrees bigint, wind_speed_kt bigint, wind_gust_kt bigint"
)

_log = get_logger(__name__)
_TABLES = {table.name: table for table in SILVER_TABLES}


def disruptions_path(settings: Settings) -> str:
    return f"{settings.lake.uri_for(LakeLayer.SILVER)}/{DISRUPTIONS_TABLE}"


def _table_or_none(spark: SparkSession, path: str) -> DataFrame | None:
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return spark.read.format("delta").load(path)


def _metar(spark: SparkSession, settings: Settings) -> DataFrame:
    stored = _table_or_none(spark, silver_path(settings, _TABLES["metar_observations"]))
    return stored if stored is not None else spark.createDataFrame([], METAR_SCHEMA)


RECORD_SCHEMA = (
    "flight_number string, scheduled_departure string, kind string, "
    "event_id string, correlation_id string, payload string"
)


def _record(spark: SparkSession, announcements: list[Announcement], path: str) -> None:
    rows = spark.createDataFrame(
        [
            (
                item.event.data["flight_number"],
                item.event.data["scheduled_departure_utc"],
                item.event.data["kind"],
                item.event.event_id,
                item.correlation_id,
                item.event.to_json().decode(),
            )
            for item in announcements
        ],
        RECORD_SCHEMA,
    )
    typed = (
        rows.withColumn("scheduled_departure_utc", functions.to_timestamp("scheduled_departure"))
        .drop("scheduled_departure")
        .withColumn("announced_at", functions.current_timestamp())
    )
    if not DeltaTable.isDeltaTable(spark, path):
        typed.limit(0).write.format("delta").save(path)
    condition = " AND ".join(f"target.{key} = source.{key}" for key in NATURAL_KEY)
    target = DeltaTable.forPath(spark, path).alias("target")
    target.merge(typed.alias("source"), condition).whenNotMatchedInsertAll().execute()


def detect_batch(batch: DataFrame, settings: Settings, announcer: Announcer) -> int:
    spark = batch.sparkSession
    changed = batch.where(functions.col("_change_type").isin(*CURRENT_VERSIONS))
    path = disruptions_path(settings)
    airports = _table_or_none(
        spark, f"{settings.lake.uri_for(LakeLayer.SILVER)}/reference_airports"
    )
    enriched = enrich(classify(changed), _metar(spark, settings), airports)
    fresh = not_yet_announced(enriched, _table_or_none(spark, path))
    announcements = [
        Announcement.from_payload(row["payload"], row["correlation_id"])
        for row in as_payloads(fresh).collect()
    ]
    if not announcements:
        return 0
    sent = announcer.announce(announcements)
    _record(spark, announcements, path)
    _log.info("disruptions_announced", count=sent)
    return sent


def start_detector(
    spark: SparkSession, settings: Settings, announcer: Announcer, trigger: Trigger
) -> StreamingQuery:
    changes = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .load(silver_path(settings, _TABLES["flight_schedules"]))
    )

    def handle(batch: DataFrame, _: int) -> None:
        detect_batch(batch, settings, announcer)

    writer = (
        changes.writeStream.queryName("detector_flight_disruptions")
        .foreachBatch(handle)
        .option("checkpointLocation", checkpoint_path(settings, "detector_flight_disruptions"))
    )
    return trigger.apply(writer).start()
