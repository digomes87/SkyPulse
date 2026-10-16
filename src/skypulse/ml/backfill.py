"""Loads a batch of historical events through bronze into silver.

Streaming silver drops events older than its watermark (ADR 0003), so history arrives the
documented batch way: events are appended to bronze like any other record, then parsed
with the same contract schemas and MERGEd into silver on the natural keys. Rerunning it
changes nothing.
"""

from collections.abc import Sequence
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession, functions

from skypulse.common.config import Settings
from skypulse.common.logging import get_logger
from skypulse.contracts import Event, EventSource
from skypulse.ingestion.metar.models import MetarObservation
from skypulse.ingestion.schedules.models import FlightSchedule
from skypulse.ml.history import SyntheticHistory
from skypulse.streaming.bronze import BRONZE_COLUMNS, PARTITION_COLUMNS, bronze_path
from skypulse.streaming.silver import SILVER_TABLES, merge_into, parse, silver_path, valid_rows

BRONZE_SCHEMA = (
    "topic string, partition int, offset long, key string, value string, "
    "correlation_id string, dlq_reason string"
)

_log = get_logger(__name__)
_TABLES = {table.name: table for table in SILVER_TABLES}


def _bronze_rows(
    spark: SparkSession, events: Sequence[Event], topic: str, correlation_id: str
) -> DataFrame:
    rows = [
        (topic, 0, index, str(event.data.get("flight_number") or event.data.get("station_icao")),
         event.to_json().decode(), correlation_id, None)
        for index, event in enumerate(events)
    ]  # fmt: skip
    return (
        spark.createDataFrame(rows, BRONZE_SCHEMA)
        .withColumn("kafka_timestamp", functions.current_timestamp())
        .withColumn("ingested_at", functions.current_timestamp())
        .withColumn("ingest_date", functions.to_date("ingested_at"))
        .select(*BRONZE_COLUMNS)
    )


def load_events(
    spark: SparkSession, settings: Settings, table_name: str, events: Sequence[Event]
) -> int:
    table = _TABLES[table_name]
    correlation_id = f"backfill-{datetime.now(UTC):%Y%m%dT%H%M%S}"
    bronze = _bronze_rows(spark, events, table.topic(), correlation_id).cache()
    bronze.write.format("delta").mode("append").partitionBy(*PARTITION_COLUMNS).save(
        bronze_path(settings)
    )
    rows = valid_rows(parse(bronze, table), table).dropDuplicates(["event_id"])
    merge_into(spark, rows, silver_path(settings, table), table.policy)
    count = bronze.count()
    bronze.unpersist()
    return count


def backfill(spark: SparkSession, settings: Settings, history: SyntheticHistory) -> dict[str, int]:
    flights, metar = history.generate()
    produced_at = datetime.now(UTC)
    schedule_events = [FlightSchedule(flight).to_event(EventSource.REPLAY) for flight in flights]
    metar_events = [
        MetarObservation.from_payload(payload).to_event(EventSource.REPLAY, produced_at)
        for payload in metar
    ]
    loaded = {
        "flight_schedules": load_events(spark, settings, "flight_schedules", schedule_events),
        "metar_observations": load_events(spark, settings, "metar_observations", metar_events),
    }
    _log.info("history_backfilled", days=history.days, **loaded)
    return loaded
