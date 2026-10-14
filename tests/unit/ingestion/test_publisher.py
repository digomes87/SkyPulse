import json
from datetime import UTC, datetime

from skypulse.common.correlation import CorrelationId, correlation_scope
from skypulse.common.streams import CORRELATION_HEADER, InMemoryStream
from skypulse.contracts import Event, EventSource, EventType
from skypulse.ingestion.metar.models import MetarObservation
from skypulse.ingestion.publisher import DEFAULT_ROUTES, DLQ_REASON_HEADER, EventPublisher

PRODUCED_AT = datetime(2026, 10, 14, 10, 0, tzinfo=UTC)
PAYLOAD = {
    "icaoId": "EIDW",
    "obsTime": 1791970200,
    "metarType": "METAR",
    "wdir": 190,
    "wspd": 15,
    "visib": "6+",
    "rawOb": "METAR EIDW 140930Z 19015KT 9999 15/10 Q1019",
}


def valid_event() -> Event:
    return MetarObservation.from_payload(PAYLOAD).to_event(EventSource.REPLAY, PRODUCED_AT)


def invalid_event() -> Event:
    data = {**MetarObservation.from_payload(PAYLOAD).data, "station_icao": "nope"}
    return Event.create(EventType.METAR_OBSERVATION, EventSource.REPLAY, "x", data, PRODUCED_AT)


async def test_valid_events_go_to_their_topic_keyed_by_station() -> None:
    stream = InMemoryStream()
    publisher = EventPublisher(stream, DEFAULT_ROUTES)

    with correlation_scope(CorrelationId("cycle-1")):
        report = await publisher.publish([valid_event()])

    records = [
        item async for item in stream.consume(DEFAULT_ROUTES.topic_for(EventType.METAR_OBSERVATION))
    ]
    assert report.published == 1
    assert report.rejected == 0
    assert str(records[0].key) == "EIDW"
    assert json.loads(records[0].payload)["event_type"] == "metar_observation"
    assert records[0].headers[CORRELATION_HEADER] == "cycle-1"


async def test_contract_violations_go_to_the_dead_letter_topic() -> None:
    stream = InMemoryStream()
    publisher = EventPublisher(stream, DEFAULT_ROUTES)

    report = await publisher.publish([invalid_event(), valid_event()])

    dead = [item async for item in stream.consume(DEFAULT_ROUTES.dead_letter)]
    assert report.published == 1
    assert report.rejected == 1
    assert "station_icao" in dead[0].headers[DLQ_REASON_HEADER]
    assert dead[0].headers[CORRELATION_HEADER]


def test_every_raw_event_type_has_a_route() -> None:
    for event_type in EventType:
        if event_type is EventType.FLIGHT_DISRUPTION:
            continue
        assert DEFAULT_ROUTES.topic_for(event_type).name.startswith("skypulse.raw.")
