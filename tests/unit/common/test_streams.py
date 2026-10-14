import pytest

from skypulse.common.correlation import CorrelationId
from skypulse.common.streams import (
    CORRELATION_HEADER,
    InMemoryStream,
    PartitionKey,
    StreamRecord,
    Topic,
)

STATES = Topic("skypulse.raw.aircraft-states")


def record(key: str, payload: bytes = b"{}") -> StreamRecord:
    return StreamRecord.create(PartitionKey(key), payload, CorrelationId("cid-1"))


@pytest.mark.parametrize("name", ["", "has space", "x" * 250, "slash/name"])
def test_topic_rejects_names_kafka_and_kinesis_cannot_hold(name: str) -> None:
    with pytest.raises(ValueError, match="topic"):
        Topic(name)


def test_partition_key_rejects_blank_values() -> None:
    with pytest.raises(ValueError, match="partition key"):
        PartitionKey("")


def test_record_carries_the_correlation_id_as_a_header() -> None:
    created = record("4ca7b5")

    assert created.correlation_id() == CorrelationId("cid-1")
    assert created.headers[CORRELATION_HEADER] == "cid-1"


def test_record_without_correlation_header_has_no_correlation_id() -> None:
    bare = StreamRecord(PartitionKey("4ca7b5"), b"{}")

    assert bare.correlation_id() is None


async def test_in_memory_stream_returns_records_in_publish_order() -> None:
    stream = InMemoryStream()

    await stream.publish(STATES, record("a", b"1"))
    await stream.publish(STATES, record("b", b"2"))
    await stream.flush()

    received = [item.payload async for item in stream.consume(STATES)]
    assert received == [b"1", b"2"]


async def test_in_memory_stream_keeps_topics_apart() -> None:
    stream = InMemoryStream()
    other = Topic("skypulse.raw.metar")

    await stream.publish(STATES, record("a"))

    assert [item async for item in stream.consume(other)] == []
