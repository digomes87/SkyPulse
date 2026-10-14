"""OpenSky Network payloads turned into contract-shaped events."""

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from skypulse.common.clock import from_unix, utc_iso
from skypulse.contracts import Event, EventSource, EventType

# Order of the state vector array returned by /states/all. Index 17 (category) only
# appears with extended=1 and is not part of the contract.
STATE_VECTOR_FIELDS = (
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity", "true_track",
    "vertical_rate", "sensors", "geo_altitude", "squawk", "spi", "position_source",
)  # fmt: skip

# (upper bound in square degrees, credits) from the OpenSky REST documentation.
_CREDIT_BANDS = ((25.0, 1), (100.0, 2), (400.0, 3))
_MAXIMUM_CREDITS = 4


@dataclass(frozen=True)
class Coordinate:
    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        if not -90 <= self.latitude <= 90:
            raise ValueError(f"latitude out of range: {self.latitude}")
        if not -180 <= self.longitude <= 180:
            raise ValueError(f"longitude out of range: {self.longitude}")


@dataclass(frozen=True)
class BoundingBox:
    south_west: Coordinate
    north_east: Coordinate

    def __post_init__(self) -> None:
        if self.south_west.latitude >= self.north_east.latitude:
            raise ValueError("south-west corner must lie south of the north-east corner")
        if self.south_west.longitude >= self.north_east.longitude:
            raise ValueError("south-west corner must lie west of the north-east corner")

    def area_square_degrees(self) -> float:
        height = self.north_east.latitude - self.south_west.latitude
        width = self.north_east.longitude - self.south_west.longitude
        return height * width

    def credit_cost(self) -> int:
        area = self.area_square_degrees()
        return next((cost for limit, cost in _CREDIT_BANDS if area <= limit), _MAXIMUM_CREDITS)

    def as_query(self) -> dict[str, float]:
        return {
            "lamin": self.south_west.latitude,
            "lomin": self.south_west.longitude,
            "lamax": self.north_east.latitude,
            "lomax": self.north_east.longitude,
        }


EUROPE = BoundingBox(Coordinate(34.0, -25.0), Coordinate(72.0, 45.0))


def _clean_callsign(raw: object) -> str | None:
    if not isinstance(raw, str):
        return None
    return raw.strip() or None


@dataclass(frozen=True)
class AircraftState:
    data: Mapping[str, Any]

    @classmethod
    def from_row(cls, row: Sequence[Any], snapshot_time: int) -> "AircraftState":
        if len(row) < len(STATE_VECTOR_FIELDS):
            raise ValueError(
                f"state vector needs {len(STATE_VECTOR_FIELDS)} fields, got {len(row)}"
            )
        named = dict(zip(STATE_VECTOR_FIELDS, row, strict=False))
        named.pop("sensors")
        named["callsign"] = _clean_callsign(named["callsign"])
        return cls({**named, "snapshot_time": snapshot_time})

    def to_event(self, source: EventSource, produced_at: datetime) -> Event:
        natural_key = f"{self.data['icao24']}|{self.data['last_contact']}"
        return Event.create(EventType.AIRCRAFT_STATE, source, natural_key, self.data, produced_at)


@dataclass(frozen=True)
class ObservedFlight:
    data: Mapping[str, Any]

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "ObservedFlight":
        return cls(
            {
                "icao24": payload["icao24"],
                "callsign": _clean_callsign(payload.get("callsign")),
                "first_seen": utc_iso(from_unix(payload["firstSeen"])),
                "last_seen": utc_iso(from_unix(payload["lastSeen"])),
                "est_departure_airport": payload.get("estDepartureAirport"),
                "est_arrival_airport": payload.get("estArrivalAirport"),
            }
        )

    def to_event(self, source: EventSource, produced_at: datetime) -> Event:
        natural_key = f"{self.data['icao24']}|{self.data['first_seen']}"
        return Event.create(EventType.OBSERVED_FLIGHT, source, natural_key, self.data, produced_at)


@dataclass(frozen=True)
class StateSnapshot:
    """All state vectors returned by one /states/all call."""

    states: tuple[AircraftState, ...]

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "StateSnapshot":
        rows = payload.get("states") or []
        snapshot_time = int(payload["time"])
        return cls(tuple(AircraftState.from_row(row, snapshot_time) for row in rows))

    def __len__(self) -> int:
        return len(self.states)

    def __iter__(self) -> Iterator[AircraftState]:
        return iter(self.states)
