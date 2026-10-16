"""Silver: typed, validated, deduplicated tables built from bronze.

Per table, two streaming queries read the bronze Delta table:

- ``silver_<table>``: parse with the contract schema, keep valid rows, drop duplicates on
  ``event_id`` within the watermark, then MERGE into the table on its natural key. The MERGE
  makes the sink idempotent, so a replayed micro-batch or a duplicate older than the
  watermark changes nothing.
- ``quarantine_<table>``: rows that fail parsing or required-field checks, appended with
  the reason. Bronze keeps the original, so quarantined data can be replayed after a fix.

Rows later than the watermark are dropped by the deduplication state and never reach
silver. They stay in bronze, where a batch backfill can pick them up.
"""

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window, functions

from skypulse.common.config import LakeLayer, Settings
from skypulse.contracts import SCHEMA_VERSION, EventType
from skypulse.streaming.schemas import event_schema, required_data_fields
from skypulse.streaming.tables import SILVER_TABLES, EventTiming, MergePolicy, SilverTable

__all__ = [
    "SILVER_TABLES",
    "EventTiming",
    "MergePolicy",
    "SilverTable",
    "deduplicated",
    "latest_per_key",
    "merge_into",
    "parse",
    "quarantine_path",
    "quarantined_rows",
    "silver_path",
    "valid_rows",
]

QUARANTINE_TABLE = "quarantine"
LINEAGE_COLUMNS = ("correlation_id", "topic", "partition", "offset", "ingested_at")


def _violations(event_type: EventType) -> Column:
    checks = [
        (functions.try_parse_json("value").isNull(), "payload is not valid JSON"),
        (functions.col("event.event_type") != event_type.value, "unexpected event_type"),
        (functions.col("event.schema_version") != SCHEMA_VERSION, "unsupported schema_version"),
        (functions.col("event.event_id").isNull(), "missing event_id"),
        *(
            (functions.col(f"event.data.{field}").isNull(), f"missing data.{field}")
            for field in required_data_fields(event_type)
        ),
    ]
    flagged = [functions.when(condition, functions.lit(reason)) for condition, reason in checks]
    return functions.filter(functions.array(*flagged), lambda reason: reason.isNotNull())


def parse(bronze: DataFrame, table: SilverTable) -> DataFrame:
    """Bronze rows of the table's topic with the parsed event and a list of violations."""
    return (
        bronze.where(functions.col("topic") == table.topic())
        .withColumn("event", functions.from_json("value", event_schema(table.event_type)))
        .withColumn("violations", _violations(table.event_type))
    )


def valid_rows(parsed: DataFrame, table: SilverTable) -> DataFrame:
    return (
        parsed.where(functions.size("violations") == 0)
        .select(
            "event.event_id",
            "event.source",
            "event.produced_at",
            "event.data.*",
            *LINEAGE_COLUMNS,
        )
        .withColumn("event_time", functions.expr(table.timing.expression))
    )


def quarantined_rows(parsed: DataFrame, table: SilverTable) -> DataFrame:
    return parsed.where(functions.size("violations") > 0).select(
        functions.lit(table.name).alias("silver_table"),
        functions.array_join("violations", "; ").alias("reason"),
        "value",
        *LINEAGE_COLUMNS,
    )


def deduplicated(rows: DataFrame, table: SilverTable) -> DataFrame:
    return rows.withWatermark("event_time", table.timing.watermark).dropDuplicatesWithinWatermark(
        ["event_id"]
    )


def latest_per_key(batch: DataFrame, policy: MergePolicy) -> DataFrame:
    """One row per natural key, so MERGE never sees two source rows for one target row."""
    order = [functions.col("ingested_at").desc(), functions.col("event_id")]
    if policy.newer_by is not None:
        order.insert(0, functions.col(policy.newer_by).desc())
    window = Window.partitionBy(*policy.keys).orderBy(*order)
    return (
        batch.withColumn("_rank", functions.row_number().over(window))
        .where("_rank = 1")
        .drop("_rank")
    )


def merge_into(spark: SparkSession, batch: DataFrame, path: str, policy: MergePolicy) -> None:
    source = latest_per_key(batch, policy)
    if not DeltaTable.isDeltaTable(spark, path):
        source.limit(0).write.format("delta").save(path)
    merge = (
        DeltaTable.forPath(spark, path)
        .alias("target")
        .merge(source.alias("source"), policy.condition())
    )
    update_condition = policy.update_condition()
    if update_condition is not None:
        merge = merge.whenMatchedUpdateAll(condition=update_condition)
    merge.whenNotMatchedInsertAll().execute()


def silver_path(settings: Settings, table: SilverTable) -> str:
    return f"{settings.lake.uri_for(LakeLayer.SILVER)}/{table.name}"


def quarantine_path(settings: Settings) -> str:
    return f"{settings.lake.uri_for(LakeLayer.SILVER)}/{QUARANTINE_TABLE}"
