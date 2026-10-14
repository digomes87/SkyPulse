from collections.abc import Callable
from typing import Any

import pytest

from skypulse.common.correlation import CorrelationId
from skypulse.common.kafka import (
    PRODUCER_DEFAULTS,
    DeliveryFailedError,
    KafkaStreamSink,
)
from skypulse.common.streams import PartitionKey, StreamRecord, Topic

TOPIC = Topic("skypulse.raw.metar")
DeliveryCallback = Callable[[Any, Any], None]


class FakeMessage:
    def __init__(self, topic: str) -> None:
        self._topic = topic

    def topic(self) -> str:
        return self._topic


class FakeProducer:
    def __init__(self, failure: str | None = None, full_queue_once: bool = False) -> None:
        self.sent: list[dict[str, Any]] = []
        self.pending: list[tuple[DeliveryCallback, str]] = []
        self.failure = failure
        self.full_queue_once = full_queue_once

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        *,
        on_delivery: DeliveryCallback | None = None,
        headers: list[tuple[str, str | bytes | None]] | None = None,
    ) -> None:
        if self.full_queue_once:
            self.full_queue_once = False
            raise BufferError("queue full")
        self.sent.append({"topic": topic, "key": key, "value": value, "headers": headers})
        assert on_delivery is not None
        self.pending.append((on_delivery, topic))

    def poll(self, timeout: float) -> int:
        return self._deliver()

    def flush(self, timeout: float) -> int:
        self._deliver()
        return 0

    def _deliver(self) -> int:
        delivered = len(self.pending)
        for callback, topic in self.pending:
            callback(self.failure, FakeMessage(topic))
        self.pending = []
        return delivered


def record() -> StreamRecord:
    return StreamRecord.create(PartitionKey("EIDW"), b'{"a":1}', CorrelationId("cid-9"))


def test_defaults_enable_idempotent_durable_writes() -> None:
    assert PRODUCER_DEFAULTS["enable.idempotence"] is True
    assert PRODUCER_DEFAULTS["acks"] == "all"


async def test_publish_sends_key_value_and_headers() -> None:
    producer = FakeProducer()
    sink = KafkaStreamSink(producer)

    await sink.publish(TOPIC, record())
    await sink.flush()

    sent = producer.sent[0]
    assert sent["topic"] == "skypulse.raw.metar"
    assert sent["key"] == b"EIDW"
    assert sent["value"] == b'{"a":1}'
    assert sent["headers"] == [("correlation-id", b"cid-9")]


async def test_flush_raises_when_a_delivery_failed() -> None:
    sink = KafkaStreamSink(FakeProducer(failure="broker down"))

    await sink.publish(TOPIC, record())

    with pytest.raises(DeliveryFailedError, match="broker down"):
        await sink.flush()


async def test_publish_waits_for_room_when_the_local_queue_is_full() -> None:
    producer = FakeProducer(full_queue_once=True)
    sink = KafkaStreamSink(producer)

    await sink.publish(TOPIC, record())
    await sink.flush()

    assert len(producer.sent) == 1
