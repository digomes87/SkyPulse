"""Contract-valid event payloads for silver tests, built with the producer code."""

from datetime import UTC, datetime, timedelta

from skypulse.contracts import EventSource
from skypulse.ingestion.metar.models import MetarObservation
from skypulse.ingestion.opensky.models import AircraftState

BASE = datetime(2026, 10, 14, 10, 0, tzinfo=UTC)


def aircraft_state(icao24: str, seen_at: datetime) -> str:
    seconds = int(seen_at.timestamp())
    row = [icao24, "RYR8TK", "Ireland", seconds, seconds, -7.6, 52.3, 11000.0, False,
           220.0, 280.0, 0.0, None, 11300.0, "2120", False, 0]  # fmt: skip
    event = AircraftState.from_row(row, snapshot_time=seconds).to_event(EventSource.REPLAY, BASE)
    return event.to_json().decode()


def metar(station: str, minutes_after_base: int, gust: int | None = None) -> str:
    observed = BASE + timedelta(minutes=minutes_after_base)
    payload = {
        "icaoId": station,
        "obsTime": int(observed.timestamp()),
        "metarType": "METAR",
        "wdir": 200,
        "wspd": 10,
        "wgst": gust,
        "visib": "6+",
        "rawOb": f"METAR {station} 9999",
    }
    return (
        MetarObservation.from_payload(payload).to_event(EventSource.REPLAY, BASE).to_json().decode()
    )
