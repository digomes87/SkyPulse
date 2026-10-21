"""Backfills silver from bronze: recovers the events the streaming watermark dropped.

The silver streams drop events that arrive later than their watermark (ADR 0003); bronze
keeps them. This job reads the bronze rows ingested since its last successful run, applies
the same parsing and validation as the streams, deduplicates on ``event_id`` without a
watermark and MERGEs into silver. The MERGE is idempotent, so rows silver already holds
change nothing and a rerun is safe. Recovered rows reach the disruption detector through
the silver change data feed.

The window is ``[last cutoff - overlap, run start)`` on ``ingested_at``. The cutoff only
moves forward after every table succeeded, so a failed run loses nothing; the overlap
covers micro-batches that were stamped before the cutoff but committed after the read.
``--full`` ignores the cutoff and reprocesses all of bronze (the full-recompute path).
"""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions

from skypulse.common.config import LakeLayer, Settings
from skypulse.common.logging import get_logger
from skypulse.observability.metrics import LATE_ROWS_RECOVERED, push_registry
from skypulse.streaming.bronze import bronze_path
from skypulse.streaming.silver import (
    SILVER_TABLES,
    SilverTable,
    merge_into,
    parse,
    silver_path,
    valid_rows,
)

STATE_TABLE = "backfill_state"
STATE_SCHEMA = "cutoff timestamp"
OVERLAP = timedelta(hours=1)

_log = get_logger(__name__)


def state_path(settings: Settings) -> str:
    return f"{settings.lake.uri_for(LakeLayer.CHECKPOINTS)}/{STATE_TABLE}"


def last_cutoff(spark: SparkSession, settings: Settings) -> datetime | None:
    path = state_path(settings)
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    row = spark.read.format("delta").load(path).first()
    return None if row is None else row["cutoff"].replace(tzinfo=UTC)


def save_cutoff(spark: SparkSession, settings: Settings, cutoff: datetime) -> None:
    frame = spark.createDataFrame([(cutoff,)], STATE_SCHEMA)
    frame.write.format("delta").mode("overwrite").save(state_path(settings))


def bronze_window(bronze: DataFrame, since: datetime | None, until: datetime) -> DataFrame:
    """Bronze rows ingested in ``[since - overlap, until)``; all of them when ``since`` is None."""
    window = bronze.where(functions.col("ingested_at") < functions.lit(until))
    if since is None:
        return window
    start = since - OVERLAP
    return window.where(functions.col("ingest_date") >= functions.lit(start.date())).where(
        functions.col("ingested_at") >= functions.lit(start)
    )


def table_version(spark: SparkSession, path: str) -> int:
    """Latest commit version of the table at ``path``; -1 when it does not exist yet."""
    if not DeltaTable.isDeltaTable(spark, path):
        return -1
    last = DeltaTable.forPath(spark, path).history(1).first()
    return -1 if last is None else int(last["version"])


def inserted_since(spark: SparkSession, path: str, version: int) -> int:
    """Rows inserted by a MERGE committed after ``version``.

    A MERGE that matches every source row and inserts nothing writes no commit at all, so
    the latest commit only counts when it is newer than the version read before the MERGE.
    """
    last = DeltaTable.forPath(spark, path).history(1).first()
    if last is None or int(last["version"]) <= version or last["operation"] != "MERGE":
        return 0
    return int(last["operationMetrics"].get("numTargetRowsInserted", "0"))


def backfill_table(
    spark: SparkSession, bronze: DataFrame, settings: Settings, table: SilverTable
) -> int:
    rows = valid_rows(parse(bronze, table), table).dropDuplicates(["event_id"])
    path = silver_path(settings, table)
    before = table_version(spark, path)
    merge_into(spark, rows, path, table.policy)
    return inserted_since(spark, path, before)


def run_backfill(
    spark: SparkSession, settings: Settings, now: datetime, full: bool = False
) -> Mapping[str, int]:
    since = None if full else last_cutoff(spark, settings)
    bronze = bronze_window(spark.read.format("delta").load(bronze_path(settings)), since, now)
    recovered = {
        table.name: backfill_table(spark, bronze, settings, table) for table in SILVER_TABLES
    }
    save_cutoff(spark, settings, now)
    for table, rows in recovered.items():
        LATE_ROWS_RECOVERED.labels(table).set(rows)
    push_registry(settings, "batch_backfill")
    _log.info("backfill_completed", since=str(since), until=str(now), recovered=recovered)
    return recovered
