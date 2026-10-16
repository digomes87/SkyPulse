"""Streaming queries that move data bronze -> silver."""

import time
from collections.abc import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from skypulse.common.config import Settings
from skypulse.streaming.bronze import bronze_path, checkpoint_path
from skypulse.streaming.silver import (
    SilverTable,
    deduplicated,
    merge_into,
    parse,
    quarantine_path,
    quarantined_rows,
    silver_path,
    valid_rows,
)
from skypulse.streaming.triggers import Trigger

TABLE_WAIT_SECONDS = 600.0
TABLE_POLL_SECONDS = 5.0


def wait_for_table(
    spark: SparkSession,
    path: str,
    timeout_seconds: float = TABLE_WAIT_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Continuous stages can start before their upstream table exists; wait for it."""
    waited = 0.0
    while not DeltaTable.isDeltaTable(spark, path):
        if waited >= timeout_seconds:
            raise TimeoutError(f"{path} did not appear within {timeout_seconds:.0f}s")
        sleep(TABLE_POLL_SECONDS)
        waited += TABLE_POLL_SECONDS


def _bronze_stream(spark: SparkSession, settings: Settings) -> DataFrame:
    return spark.readStream.format("delta").load(bronze_path(settings))


def start_silver(
    spark: SparkSession, settings: Settings, table: SilverTable, trigger: Trigger
) -> StreamingQuery:
    rows = deduplicated(valid_rows(parse(_bronze_stream(spark, settings), table), table), table)
    target = silver_path(settings, table)

    def merge_batch(batch: DataFrame, _: int) -> None:
        merge_into(batch.sparkSession, batch, target, table.policy)

    writer = (
        rows.writeStream.queryName(f"silver_{table.name}")
        .foreachBatch(merge_batch)
        .option("checkpointLocation", checkpoint_path(settings, f"silver_{table.name}"))
    )
    return trigger.apply(writer).start()


def ensure_quarantine_table(spark: SparkSession, settings: Settings, table: SilverTable) -> None:
    """Create the shared quarantine table up front.

    Several quarantine queries append to it; if they all found an empty directory they
    would race to create the Delta log and all but one would fail.
    """
    path = quarantine_path(settings)
    if DeltaTable.isDeltaTable(spark, path):
        return
    bronze = spark.read.format("delta").load(bronze_path(settings))
    empty = quarantined_rows(parse(bronze, table), table).limit(0)
    empty.write.format("delta").mode("append").save(path)


def start_quarantine(
    spark: SparkSession, settings: Settings, table: SilverTable, trigger: Trigger
) -> StreamingQuery:
    rows = quarantined_rows(parse(_bronze_stream(spark, settings), table), table)
    writer = (
        rows.writeStream.format("delta")
        .queryName(f"quarantine_{table.name}")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path(settings, f"quarantine_{table.name}"))
    )
    return trigger.apply(writer).start(quarantine_path(settings))
