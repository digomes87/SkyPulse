"""Flight schedules parsed from AeroDataBox FIDS payloads (requested with withLeg=true).

In a FIDS response the leg at the requested airport carries no ``airport`` object: for a
departure the origin is the requested airport, for an arrival the destination is.
"""

import hashlib
import json
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from skypulse.common.aviation import AirportIcao
from skypulse.common.clock import utc_iso
from skypulse.common.logging import get_logger
from skypulse.contracts import Event, EventSource, EventType

MUTABLE_FIELDS = (
    "status",
    "revised_departure_utc",
    "revised_arrival_utc",
    "runway_departure_utc",
    "runway_arrival_utc",
)
_FLIGHT_NUMBER = re.compile(r"^[A-Z0-9]{2,3}[0-9]{1,4}[A-Z]?$")

_log = get_logger(__name__)


class UnusableFlightError(ValueError):
    pass


def _leg_time(leg: Mapping[str, Any], name: str) -> str | None:
    moment = leg.get(name)
    if not moment:
        return None
    return utc_iso(datetime.fromisoformat(moment["utc"]))


def _leg_airport(leg: Mapping[str, Any], requested: AirportIcao) -> str:
    airport = leg.get("airport")
    if airport is None:
        return str(requested)
    icao = airport.get("icao")
    if not icao:
        raise UnusableFlightError(f"airport without ICAO code: {airport.get('name')}")
    return str(AirportIcao(icao))


def _flight_number(raw: str) -> str:
    number = raw.replace(" ", "").upper()
    if not _FLIGHT_NUMBER.fullmatch(number):
        raise UnusableFlightError(f"unexpected flight number {raw!r}")
    return number


def _required_departure_time(departure: Mapping[str, Any]) -> str:
    scheduled = _leg_time(departure, "scheduledTime")
    if scheduled is None:
        raise UnusableFlightError("flight has no scheduled departure time")
    return scheduled


@dataclass(frozen=True)
class FlightSchedule:
    data: Mapping[str, Any]

    @classmethod
    def from_fids_item(
        cls, item: Mapping[str, Any], requested: AirportIcao, observed_at: datetime
    ) -> "FlightSchedule":
        departure = item.get("departure") or {}
        arrival = item.get("arrival") or {}
        airline = item.get("airline") or {}
        return cls(
            {
                "flight_number": _flight_number(item["number"]),
                "callsign": item.get("callSign"),
                "airline_name": airline.get("name", "unknown"),
                "airline_iata": airline.get("iata"),
                "airline_icao": airline.get("icao"),
                "origin_icao": _leg_airport(departure, requested),
                "destination_icao": _leg_airport(arrival, requested),
                "scheduled_departure_utc": _required_departure_time(departure),
                "scheduled_arrival_utc": _leg_time(arrival, "scheduledTime"),
                "revised_departure_utc": _leg_time(departure, "revisedTime"),
                "revised_arrival_utc": _leg_time(arrival, "revisedTime"),
                "runway_departure_utc": _leg_time(departure, "runwayTime"),
                "runway_arrival_utc": _leg_time(arrival, "runwayTime"),
                "status": item.get("status", "Unknown"),
                "is_cargo": bool(item.get("isCargo", False)),
                "observed_at": utc_iso(observed_at),
            }
        )

    def natural_key(self) -> str:
        mutable = json.dumps([self.data[name] for name in MUTABLE_FIELDS])
        fingerprint = hashlib.sha256(mutable.encode()).hexdigest()[:16]
        return f"{self.data['flight_number']}|{self.data['scheduled_departure_utc']}|{fingerprint}"

    def to_event(self, source: EventSource) -> Event:
        produced_at = datetime.fromisoformat(self.data["observed_at"])
        return Event.create(
            EventType.FLIGHT_SCHEDULE, source, self.natural_key(), self.data, produced_at
        )


@dataclass(frozen=True)
class ScheduleBatch:
    flights: tuple[FlightSchedule, ...]
    skipped: int = 0

    @classmethod
    def from_fids(
        cls, payload: Mapping[str, Any], requested: AirportIcao, observed_at: datetime
    ) -> "ScheduleBatch":
        items = [*payload.get("departures", []), *payload.get("arrivals", [])]
        parsed = [cls._parse(item, requested, observed_at) for item in items]
        flights = tuple(flight for flight in parsed if flight is not None)
        return cls(flights, skipped=len(items) - len(flights))

    @staticmethod
    def _parse(
        item: Mapping[str, Any], requested: AirportIcao, observed_at: datetime
    ) -> FlightSchedule | None:
        try:
            return FlightSchedule.from_fids_item(item, requested, observed_at)
        except (UnusableFlightError, KeyError) as error:
            _log.info("schedule_item_skipped", airport=str(requested), reason=str(error))
            return None

    def merged_with(self, other: "ScheduleBatch") -> "ScheduleBatch":
        return ScheduleBatch(self.flights + other.flights, self.skipped + other.skipped)

    def __len__(self) -> int:
        return len(self.flights)

    def __iter__(self) -> Iterator[FlightSchedule]:
        return iter(self.flights)
