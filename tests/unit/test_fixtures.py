"""The committed replay fixtures must keep parsing and validating with the current code."""

from pathlib import Path

import pytest

from skypulse.common.aviation import AirportIcao
from skypulse.contracts import EventSource, EventType, contract_for
from skypulse.ingestion.metar.models import MetarBatch
from skypulse.ingestion.opensky.models import StateSnapshot
from skypulse.ingestion.schedules.models import ScheduleBatch
from skypulse.ingestion.snapshots import Snapshot

FIXTURES = Path(__file__).parents[2] / "data" / "fixtures"


def snapshots(folder: str, pattern: str) -> list[Snapshot]:
    return [Snapshot(path) for path in sorted((FIXTURES / folder).glob(pattern))]


def schedule_batches() -> list[ScheduleBatch]:
    return [
        ScheduleBatch.from_fids(snap.load(), AirportIcao(snap.path.parent.name), snap.taken_at())
        for snap in snapshots("aerodatabox", "*/*.json")
    ]


def test_every_schedule_snapshot_parses_and_validates() -> None:
    batches = schedule_batches()

    assert batches
    for batch in batches:
        assert batch.skipped == 0
        for flight in batch:
            contract_for(EventType.FLIGHT_SCHEDULE).validate(flight.to_event(EventSource.REPLAY))


@pytest.mark.parametrize(
    ("number", "final_status"),
    [("EI105", "Arrived"), ("FR6403", "Arrived"), ("FR1234", "Canceled")],
)
def test_pinned_scenarios_are_present(number: str, final_status: str) -> None:
    statuses = [
        flight.data["status"]
        for batch in schedule_batches()
        for flight in batch
        if flight.data["flight_number"] == number
    ]

    assert final_status in statuses


def test_every_metar_snapshot_parses_and_validates() -> None:
    found = snapshots("metar", "*.json")

    assert found
    for snapshot in found:
        for observation in MetarBatch.from_payload(snapshot.load()):
            event = observation.to_event(EventSource.REPLAY, snapshot.taken_at())
            contract_for(EventType.METAR_OBSERVATION).validate(event)


def test_every_states_snapshot_parses_and_validates() -> None:
    found = snapshots("opensky/states", "*.json.gz")

    assert found
    for snapshot in found:
        states = StateSnapshot.from_payload(snapshot.load())
        assert len(states) > 0
        for state in states:
            contract_for(EventType.AIRCRAFT_STATE).validate(
                state.to_event(EventSource.REPLAY, snapshot.taken_at())
            )
