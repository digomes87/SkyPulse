"""Kafka implementation of StreamSink.

``publish`` hands the record to librdkafka's local queue and returns; ``flush`` waits for
every outstanding delivery and raises if any of them failed. Producers call ``flush`` at the
end of each polling cycle, which is the point where a batch counts as durable.

The producer is idempotent (``enable.idempotence``), so broker-side retries never create
duplicates within a session; cross-session duplicates are handled by event_id downstream.
"""

import asyncio
from collections.abc import Callable
from typing import Any, Protocol

from confluent_kafka import Producer

from skypulse.common.streams import StreamRecord, Topic

PRODUCER_DEFAULTS: dict[str, Any] = {
    "enable.idempotence": True,
    "acks": "all",
    "compression.type": "zstd",
    "linger.ms": 50,
    "delivery.timeout.ms": 120_000,
}
FULL_QUEUE_WAIT_SECONDS = 0.5
FLUSH_TIMEOUT_SECONDS = 30.0


class DeliveryFailedError(RuntimeError):
    pass


Headers = list[tuple[str, str | bytes | None]]
DeliveryCallback = Callable[[Any, Any], None]


class ProducerClient(Protocol):
    def produce(
        self,
        topic: str,
        value: bytes | None = ...,
        key: bytes | None = ...,
        *,
        on_delivery: DeliveryCallback | None = ...,
        headers: Headers | None = ...,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class KafkaStreamSink:
    def __init__(self, producer: ProducerClient) -> None:
        self._producer = producer
        self._failures: list[str] = []

    @classmethod
    def connect(cls, bootstrap_servers: str) -> "KafkaStreamSink":
        return cls(Producer({**PRODUCER_DEFAULTS, "bootstrap.servers": bootstrap_servers}))

    async def publish(self, topic: Topic, record: StreamRecord) -> None:
        while not self._try_produce(topic, record):
            await asyncio.to_thread(self._producer.poll, FULL_QUEUE_WAIT_SECONDS)
        self._producer.poll(0)

    async def flush(self) -> None:
        remaining = await asyncio.to_thread(self._producer.flush, FLUSH_TIMEOUT_SECONDS)
        failures, self._failures = self._failures, []
        if remaining:
            failures.append(f"{remaining} messages still queued after flush timeout")
        if failures:
            raise DeliveryFailedError("; ".join(failures))

    def _try_produce(self, topic: Topic, record: StreamRecord) -> bool:
        headers: Headers = [(name, value.encode()) for name, value in record.headers.items()]
        try:
            self._producer.produce(
                str(topic),
                record.payload,
                str(record.key).encode(),
                on_delivery=self._on_delivery,
                headers=headers,
            )
        except BufferError:
            return False
        return True

    def _on_delivery(self, error: object, message: Any) -> None:
        if error is None:
            return
        self._failures.append(f"{message.topic()}: {error}")
