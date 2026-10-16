from datetime import UTC, datetime

from skypulse.contracts import EventSource, EventType, contract_for
from skypulse.ingestion.metar.models import MetarObservation
from skypulse.ingestion.schedules.models import FlightSchedule
from skypulse.ml.history import SyntheticHistory, history_before

START = datetime(2026, 7, 1, tzinfo=UTC)


def generate(days: int = 14) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    return SyntheticHistory(START, days).generate()


def test_generation_is_deterministic() -> None:
    assert generate(3) == generate(3)


def test_every_record_fits_the_contracts() -> None:
    flights, metar = generate(2)

    for flight in flights:
        contract_for(EventType.FLIGHT_SCHEDULE).validate(
            FlightSchedule(flight).to_event(EventSource.REPLAY)
        )
    for payload in metar[:200]:
        event = MetarObservation.from_payload(payload).to_event(EventSource.REPLAY, START)
        contract_for(EventType.METAR_OBSERVATION).validate(event)


def test_storms_make_delays_longer() -> None:
    flights, metar = generate(30)
    stormy = {
        (
            payload["icaoId"],
            datetime.fromtimestamp(int(str(payload["obsTime"])) + 600, UTC).replace(minute=0),
        )
        for payload in metar
        if payload["wxString"] == "+TSRA"
    }

    def delay(flight: dict[str, object]) -> float:
        actual = datetime.fromisoformat(str(flight["runway_arrival_utc"]))
        return (
            actual - datetime.fromisoformat(str(flight["scheduled_arrival_utc"]))
        ).total_seconds() / 60

    def departs_in_storm(flight: dict[str, object]) -> bool:
        hour = datetime.fromisoformat(str(flight["scheduled_departure_utc"])).replace(minute=0)
        return (flight["origin_icao"], hour) in stormy

    landed = [flight for flight in flights if flight["status"] == "Arrived"]
    in_storm = [delay(flight) for flight in landed if departs_in_storm(flight)]
    calm = [delay(flight) for flight in landed if not departs_in_storm(flight)]

    assert in_storm
    assert sum(in_storm) / len(in_storm) > sum(calm) / len(calm) + 40


def test_history_ends_the_day_before() -> None:
    history = history_before(datetime(2026, 10, 14, 13, tzinfo=UTC), days=90)

    assert history.start == datetime(2026, 7, 16, tzinfo=UTC)
