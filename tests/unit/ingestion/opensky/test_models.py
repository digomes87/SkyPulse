from datetime import UTC, datetime

import pytest

from skypulse.contracts import EventSource, EventType, contract_for
from skypulse.ingestion.opensky.models import (
    EUROPE,
    AircraftState,
    BoundingBox,
    Coordinate,
    ObservedFlight,
)

PRODUCED_AT = datetime(2026, 10, 14, 10, 0, tzinfo=UTC)

ROW = [
    "4ca7b5", "RYR8TK  ", "Ireland", 1791972199, 1791972199, -7.6119, 52.3794,
    11582.4, False, 224.09, 280.72, 0, None, 12047.22, "2120", False, 0,
]  # fmt: skip


def test_coordinate_rejects_out_of_range_values() -> None:
    with pytest.raises(ValueError, match="latitude"):
        Coordinate(latitude=91, longitude=0)
    with pytest.raises(ValueError, match="longitude"):
        Coordinate(latitude=0, longitude=-181)


def test_bounding_box_requires_south_west_below_north_east() -> None:
    with pytest.raises(ValueError, match="south-west"):
        BoundingBox(Coordinate(56, -5), Coordinate(51, -11))


@pytest.mark.parametrize(
    ("north_east", "credits"),
    [
        (Coordinate(55, -6), 1),  # 4 x 5 = 20 sq deg
        (Coordinate(60, -1), 2),  # 9 x 10 = 90 sq deg
        (Coordinate(66, 9), 3),  # 15 x 20 = 300 sq deg
        (Coordinate(71, 19), 4),  # 20 x 30 = 600 sq deg
    ],
)
def test_credit_cost_follows_the_opensky_area_bands(north_east: Coordinate, credits: int) -> None:
    box = BoundingBox(Coordinate(51, -11), north_east)

    assert box.credit_cost() == credits


def test_bounding_box_query_parameters() -> None:
    box = BoundingBox(Coordinate(51, -11), Coordinate(56, -5))

    assert box.as_query() == {"lamin": 51, "lomin": -11, "lamax": 56, "lomax": -5}


def test_europe_box_costs_the_maximum() -> None:
    assert EUROPE.credit_cost() == 4


def test_aircraft_state_parses_a_state_vector_row() -> None:
    state = AircraftState.from_row(ROW, snapshot_time=1791972200)

    assert state.data["icao24"] == "4ca7b5"
    assert state.data["callsign"] == "RYR8TK"
    assert state.data["latitude"] == 52.3794
    assert state.data["snapshot_time"] == 1791972200


def test_aircraft_state_accepts_the_extended_row_with_category() -> None:
    state = AircraftState.from_row([*ROW, 4], snapshot_time=1791972200)

    assert state.data["icao24"] == "4ca7b5"


def test_aircraft_state_rejects_short_rows() -> None:
    with pytest.raises(ValueError, match="17"):
        AircraftState.from_row(ROW[:10], snapshot_time=1791972200)


def test_blank_callsign_becomes_null() -> None:
    row = list(ROW)
    row[1] = "        "

    assert AircraftState.from_row(row, snapshot_time=1).data["callsign"] is None


def test_aircraft_state_event_matches_its_contract() -> None:
    event = AircraftState.from_row(ROW, snapshot_time=1791972200).to_event(
        EventSource.OPENSKY, PRODUCED_AT
    )

    assert event.event_type is EventType.AIRCRAFT_STATE
    contract_for(EventType.AIRCRAFT_STATE).validate(event)


def test_same_vector_seen_in_two_snapshots_keeps_one_event_id() -> None:
    first = AircraftState.from_row(ROW, snapshot_time=1791972200)
    second = AircraftState.from_row(ROW, snapshot_time=1791972210)

    assert (
        first.to_event(EventSource.OPENSKY, PRODUCED_AT).event_id
        == second.to_event(EventSource.OPENSKY, PRODUCED_AT).event_id
    )


def test_observed_flight_parses_the_flights_payload() -> None:
    payload = {
        "icao24": "4ca7b5",
        "firstSeen": 1791900000,
        "estDepartureAirport": "EIDW",
        "lastSeen": 1791906000,
        "estArrivalAirport": "LEMD",
        "callsign": "RYR7241 ",
        "estDepartureAirportHorizDistance": 1200,
    }

    flight = ObservedFlight.from_payload(payload)
    event = flight.to_event(EventSource.OPENSKY, PRODUCED_AT)

    assert event.data["callsign"] == "RYR7241"
    assert event.data["first_seen"] == "2026-10-13T14:00:00Z"
    contract_for(EventType.OBSERVED_FLIGHT).validate(event)
