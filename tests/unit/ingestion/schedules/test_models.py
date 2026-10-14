from datetime import UTC, datetime

from skypulse.common.aviation import AirportIcao
from skypulse.contracts import EventSource, EventType, contract_for
from skypulse.ingestion.schedules.models import ScheduleBatch
from tests.unit.ingestion.schedules.fids_sample import arrival, departure, fids

DUBLIN = AirportIcao("EIDW")
OBSERVED_AT = datetime(2026, 10, 14, 8, 0, tzinfo=UTC)


def parse(payload: dict[str, object]) -> ScheduleBatch:
    return ScheduleBatch.from_fids(payload, DUBLIN, OBSERVED_AT)


def test_departure_takes_the_requested_airport_as_origin() -> None:
    flight = next(iter(parse(fids(departure()))))

    assert flight.data["flight_number"] == "FR7241"
    assert flight.data["origin_icao"] == "EIDW"
    assert flight.data["destination_icao"] == "LEMD"
    assert flight.data["scheduled_departure_utc"] == "2026-10-14T06:55:00Z"
    assert flight.data["revised_departure_utc"] == "2026-10-14T07:20:00Z"
    assert flight.data["scheduled_arrival_utc"] == "2026-10-14T09:30:00Z"
    assert flight.data["airline_icao"] == "RYR"


def test_arrival_takes_the_requested_airport_as_destination() -> None:
    flight = next(iter(parse(fids(arrivals=(arrival(),)))))

    assert flight.data["origin_icao"] == "LEMD"
    assert flight.data["destination_icao"] == "EIDW"
    assert flight.data["runway_arrival_utc"] == "2026-10-14T16:05:00Z"


def test_flights_without_icao_for_the_other_airport_are_skipped() -> None:
    batch = parse(fids(departure(destination={"iata": "XXX", "name": "Nowhere"}), departure()))

    assert len(batch) == 1
    assert batch.skipped == 1


def test_events_validate_against_the_contract() -> None:
    for flight in parse(fids(departure(), arrivals=(arrival(),))):
        event = flight.to_event(EventSource.AERODATABOX)
        contract_for(EventType.FLIGHT_SCHEDULE).validate(event)


def test_unchanged_flight_polled_twice_keeps_its_event_id() -> None:
    later = datetime(2026, 10, 14, 8, 5, tzinfo=UTC)
    first = next(iter(parse(fids(departure()))))
    second = next(iter(ScheduleBatch.from_fids(fids(departure()), DUBLIN, later)))

    assert first.to_event(EventSource.AERODATABOX).event_id == (
        second.to_event(EventSource.AERODATABOX).event_id
    )


def test_a_status_change_produces_a_new_event_id() -> None:
    boarding = next(iter(parse(fids(departure(status="Boarding")))))
    departed = next(iter(parse(fids(departure(status="Departed")))))

    assert boarding.to_event(EventSource.AERODATABOX).event_id != (
        departed.to_event(EventSource.AERODATABOX).event_id
    )
