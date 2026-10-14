"""Builds DataFrames shaped like Spark's Kafka source, for transformation tests."""

from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType()),
        StructField(
            "headers",
            ArrayType(
                StructType([StructField("key", StringType()), StructField("value", BinaryType())])
            ),
        ),
    ]
)

KAFKA_TIME = datetime(2026, 10, 14, 10, 0, tzinfo=UTC)


def kafka_frame(
    spark: SparkSession, topic: str, values: list[str], headers: dict[str, str] | None = None
) -> DataFrame:
    header_rows = [(name, value.encode()) for name, value in (headers or {}).items()]
    rows = [
        (b"key", value.encode(), topic, 0, offset, KAFKA_TIME, 0, header_rows)
        for offset, value in enumerate(values)
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)
