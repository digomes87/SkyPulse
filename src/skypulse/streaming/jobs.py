"""Streaming queries that move data bronze -> silver."""

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
