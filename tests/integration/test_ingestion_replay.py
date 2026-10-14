"""Producers in replay mode publish contract-valid events to the local Kafka."""

import json
import uuid
from pathlib import Path

import pytest
from confluent_kafka import Consumer

from skypulse.common.config import Settings
from skypulse.contracts import EventType
from skypulse.ingestion.__main__ import main
from skypulse.ingestion.publisher import DEFAULT_ROUTES

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).parents[2] / "data" / "fixtures"


@pytest.fixture(autouse=True)
def replay_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SKYPULSE_INGESTION__REPLAY", "true")
    monkeypatch.setenv("SKYPULSE_INGESTION__FIXTURES_DIR", str(FIXTURES))
    monkeypatch.setenv("SKYPULSE_INGESTION__REPLAY_START", "2026-10-14T13:30:00Z")


def consume_all(topic: str, minimum: int) -> list[dict[str, object]]:
    consumer = Consumer(
        {
            "bootstrap.servers": Settings().kafka.bootstrap_servers,
            "group.id": f"it-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    documents: list[dict[str, object]] = []
    empty_polls = 0
    while len(documents) < minimum and empty_polls < 20:
        message = consumer.poll(1.0)
        value = None if message is None or message.error() else message.value()
        if value is None:
            empty_polls += 1
            continue
        documents.append(json.loads(value))
    consumer.close()
    return documents


@pytest.mark.parametrize(
    ("feed", "event_type"),
    [
        ("states", EventType.AIRCRAFT_STATE),
        ("schedules", EventType.FLIGHT_SCHEDULE),
        ("metar", EventType.METAR_OBSERVATION),
    ],
)
async def test_feed_publishes_to_its_topic(feed: str, event_type: EventType) -> None:
    assert await main([feed, "--cycles", "1"]) == 0

    documents = consume_all(str(DEFAULT_ROUTES.topic_for(event_type)), minimum=1)

    assert documents
    assert {document["event_type"] for document in documents} == {event_type.value}
    assert {document["source"] for document in documents} == {"replay"}
