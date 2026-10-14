import pytest
from pyspark.sql import SparkSession

from skypulse.streaming.bronze import BRONZE_COLUMNS, to_bronze
from tests.unit.streaming.kafka_rows import kafka_frame

pytestmark = pytest.mark.spark


def test_bronze_keeps_the_raw_payload_and_kafka_coordinates(spark: SparkSession) -> None:
    frame = kafka_frame(
        spark,
        "skypulse.raw.metar",
        ['{"event_id": "e1"}', "not even json"],
        headers={"correlation-id": "cid-7"},
    )

    rows = to_bronze(frame).orderBy("offset").collect()

    assert rows[0]["value"] == '{"event_id": "e1"}'
    assert rows[1]["value"] == "not even json"
    assert rows[0]["topic"] == "skypulse.raw.metar"
    assert rows[0]["key"] == "key"
    assert rows[0]["correlation_id"] == "cid-7"
    assert rows[0]["dlq_reason"] is None
    assert rows[0]["ingest_date"] is not None


def test_bronze_has_a_stable_column_layout(spark: SparkSession) -> None:
    frame = kafka_frame(spark, "skypulse.dlq.raw", ["{}"], headers={"dlq-reason": "bad field"})

    bronze = to_bronze(frame)

    assert bronze.columns == list(BRONZE_COLUMNS)
    assert bronze.first()["dlq_reason"] == "bad field"  # type: ignore[index]
