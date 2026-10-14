"""Validates events against their contract and puts them on the stream.

A contract violation never stops ingestion: the event is written to the dead-letter topic
with the reason in a header, and the rest of the batch carries on.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from skypulse.common.correlation import CorrelationId, current_correlation_id
from skypulse.common.logging import get_logger
from skypulse.common.streams import (
    CORRELATION_HEADER,
    PartitionKey,
    StreamRecord,
    StreamSink,
    Topic,
)
from skypulse.contracts import ContractViolationError, Event, EventType, contract_for

DLQ_REASON_HEADER = "dlq-reason"

_log = get_logger(__name__)


@dataclass(frozen=True)
class Route:
    topic: Topic
    key_field: str


@dataclass(frozen=True)
class Routes:
    by_type: Mapping[EventType, Route]
    dead_letter: Topic

    def topic_for(self, event_type: EventType) -> Topic:
        return self.by_type[event_type].topic

    def key_for(self, event: Event) -> PartitionKey:
        field = self.by_type[event.event_type].key_field
        return PartitionKey(str(event.data.get(field) or event.event_id))


DEFAULT_ROUTES = Routes(
    {
        EventType.AIRCRAFT_STATE: Route(Topic("skypulse.raw.aircraft-states"), "icao24"),
        EventType.OBSERVED_FLIGHT: Route(Topic("skypulse.raw.observed-flights"), "icao24"),
        EventType.FLIGHT_SCHEDULE: Route(Topic("skypulse.raw.flight-schedules"), "flight_number"),
        EventType.METAR_OBSERVATION: Route(Topic("skypulse.raw.metar"), "station_icao"),
    },
    dead_letter=Topic("skypulse.dlq.raw"),
)


@dataclass(frozen=True)
class PublishReport:
    published: int
    rejected: int

    def plus(self, other: "PublishReport") -> "PublishReport":
        return PublishReport(self.published + other.published, self.rejected + other.rejected)


class EventPublisher:
    def __init__(self, sink: StreamSink, routes: Routes) -> None:
        self._sink = sink
        self._routes = routes

    async def publish(self, events: Iterable[Event]) -> PublishReport:
        correlation_id = current_correlation_id() or CorrelationId.new()
        report = PublishReport(0, 0)
        for event in events:
            report = report.plus(await self._publish_one(event, correlation_id))
        await self._sink.flush()
        return report

    async def _publish_one(self, event: Event, correlation_id: CorrelationId) -> PublishReport:
        key = self._routes.key_for(event)
        try:
            contract_for(event.event_type).validate(event)
        except ContractViolationError as violation:
            await self._dead_letter(event, key, correlation_id, str(violation))
            return PublishReport(0, 1)
        record = StreamRecord.create(key, event.to_json(), correlation_id)
        await self._sink.publish(self._routes.topic_for(event.event_type), record)
        return PublishReport(1, 0)

    async def _dead_letter(
        self, event: Event, key: PartitionKey, correlation_id: CorrelationId, reason: str
    ) -> None:
        _log.warning("event_dead_lettered", event_type=str(event.event_type), reason=reason)
        headers = {CORRELATION_HEADER: str(correlation_id), DLQ_REASON_HEADER: reason}
        await self._sink.publish(
            self._routes.dead_letter, StreamRecord(key, event.to_json(), headers)
        )
