"""Bronze: every Kafka record, untouched, in one append-only Delta table.

Schema-on-read: the payload is kept as a string, so a producer bug or a contract change can
never make bronze reject data. Kafka coordinates are kept for lineage and replays.
"""

from pyspark.sql import Column, DataFrame, SparkSession, functions
from pyspark.sql.streaming.query import StreamingQuery

from skypulse.common.config import LakeLayer, Settings
from skypulse.streaming.triggers import Trigger

RAW_EVENTS_TABLE = "raw_events"
SUBSCRIBE_PATTERN = r"skypulse\.(raw|dlq)\..*"
PARTITION_COLUMNS = ("topic", "ingest_date")
BRONZE_COLUMNS = (
    "topic",
    "partition",
    "offset",
    "key",
    "value",
    "correlation_id",
    "dlq_reason",
    "kafka_timestamp",
    "ingested_at",
    "ingest_date",
)


def _header(name: str) -> Column:
    matching = functions.filter("headers", lambda header: header["key"] == functions.lit(name))
    return functions.try_element_at(matching, functions.lit(1))["value"].cast("string")


def to_bronze(kafka: DataFrame) -> DataFrame:
    return (
        kafka.withColumn("key", functions.col("key").cast("string"))
        .withColumn("value", functions.col("value").cast("string"))
        .withColumn("correlation_id", _header("correlation-id"))
        .withColumn("dlq_reason", _header("dlq-reason"))
        .withColumn("kafka_timestamp", functions.col("timestamp"))
        .withColumn("ingested_at", functions.current_timestamp())
        .withColumn("ingest_date", functions.to_date("ingested_at"))
        .select(*BRONZE_COLUMNS)
    )


def bronze_path(settings: Settings) -> str:
    return f"{settings.lake.uri_for(LakeLayer.BRONZE)}/{RAW_EVENTS_TABLE}"


def checkpoint_path(settings: Settings, query_name: str) -> str:
    return f"{settings.lake.uri_for(LakeLayer.CHECKPOINTS)}/{query_name}"


def start_bronze(spark: SparkSession, settings: Settings, trigger: Trigger) -> StreamingQuery:
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribePattern", SUBSCRIBE_PATTERN)
        .option("startingOffsets", "earliest")
        .option("includeHeaders", "true")
        .load()
    )
    writer = (
        to_bronze(kafka)
        .writeStream.format("delta")
        .queryName("bronze_raw_events")
        .outputMode("append")
        .partitionBy(*PARTITION_COLUMNS)
        .option("checkpointLocation", checkpoint_path(settings, "bronze_raw_events"))
    )
    return trigger.apply(writer).start(bronze_path(settings))
