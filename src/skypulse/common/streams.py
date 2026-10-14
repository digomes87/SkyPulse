"""Transport-agnostic stream contracts.

Producers and consumers depend on ``StreamSink`` / ``StreamSource`` only. Kafka backs them
in local mode and Kinesis in AWS mode, so switching transport is a configuration change.
Names are validated against the stricter of the two systems (Kinesis: 128 characters).
"""

import re
from collections import defaultdict
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Protocol

from skypulse.common.correlation import CorrelationId

CORRELATION_HEADER = "correlation-id"

_TOPIC_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


@dataclass(frozen=True)
class Topic:
    name: str

    def __post_init__(self) -> None:
        if not _TOPIC_PATTERN.fullmatch(self.name):
            raise ValueError(f"invalid topic name {self.name!r}: use 1-128 of [A-Za-z0-9._-]")

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True)
class PartitionKey:
    """Key that pins related events (for example one aircraft) to one partition or shard."""

    value: str

    def __post_init__(self) -> None:
        if not self.value:
            raise ValueError("partition key must not be empty")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class StreamRecord:
    key: PartitionKey
    payload: bytes
    headers: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def create(
        cls, key: PartitionKey, payload: bytes, correlation_id: CorrelationId
    ) -> "StreamRecord":
        return cls(key, payload, {CORRELATION_HEADER: str(correlation_id)})

    def correlation_id(self) -> CorrelationId | None:
        raw = self.headers.get(CORRELATION_HEADER)
        if raw is None:
            return None
        return CorrelationId(raw)


class StreamSink(Protocol):
    async def publish(self, topic: Topic, record: StreamRecord) -> None: ...

    async def flush(self) -> None: ...


class StreamSource(Protocol):
    def consume(self, topic: Topic) -> AsyncIterator[StreamRecord]: ...


class InMemoryStream:
    """Sink and source in one process, for unit tests and dry runs."""

    def __init__(self) -> None:
        self._records: defaultdict[Topic, list[StreamRecord]] = defaultdict(list)

    async def publish(self, topic: Topic, record: StreamRecord) -> None:
        self._records[topic].append(record)

    async def flush(self) -> None:
        return None

    async def consume(self, topic: Topic) -> AsyncIterator[StreamRecord]:
        for record in tuple(self._records[topic]):
            yield record
