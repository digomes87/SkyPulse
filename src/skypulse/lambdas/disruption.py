"""A flight_disruption event, read back into EU261 domain objects."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from skypulse.eu261.geography import Coordinates, CountryCode
from skypulse.eu261.rules import (
    Airport,
    Arrived,
    Assessment,
    Cancelled,
    Carrier,
    FlightCase,
    InProgress,
    Outcome,
    Route,
    assess,
)
from skypulse.eu261.weather import (
    SevereCondition,
    WeatherObservation,
    WeatherPolicy,
    Wind,
    severe_weather_around,
)


def _instant(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _airport(raw: Mapping[str, Any]) -> Airport:
    return Airport(
        raw["icao"],
        CountryCode(raw["country"]),
        Coordinates(raw["latitude"], raw["longitude"]),
    )


def _observation(raw: Mapping[str, Any]) -> WeatherObservation:
    wind = Wind(raw["wind_direction_degrees"], raw["wind_speed_kt"], raw["wind_gust_kt"])
    return WeatherObservation(
        datetime.fromisoformat(raw["observed_at"]),
        raw["present_weather"],
        raw["visibility_m"],
        wind,
    )


@dataclass(frozen=True)
class Disruption:
    event_id: str
    data: Mapping[str, Any]

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "Disruption":
        if document.get("event_type") != "flight_disruption":
            raise ValueError(f"not a flight_disruption: {document.get('event_type')!r}")
        return cls(document["event_id"], document["data"])

    def claim_id(self) -> str:
        return f"{self.data['flight_number']}#{self.data['scheduled_departure_utc']}"

    def assessable(self) -> bool:
        required = ("country", "latitude", "longitude")
        airports = (self.data["origin"], self.data["destination"])
        return all(airport[field] is not None for airport in airports for field in required)

    def flight_case(self) -> FlightCase:
        route = Route(_airport(self.data["origin"]), _airport(self.data["destination"]))
        country = self.data["airline_country"]
        carrier = Carrier(self.data["airline_icao"], CountryCode(country) if country else None)
        return FlightCase(route, carrier, self._outcome())

    def severe_weather(self, policy: WeatherPolicy) -> frozenset[SevereCondition]:
        departure = _instant(self.data["scheduled_departure_utc"])
        arrival = _instant(self.data["scheduled_arrival_utc"])
        at_origin = [_observation(item) for item in self.data["origin_weather"]]
        at_destination = [_observation(item) for item in self.data["destination_weather"]]
        found = frozenset[SevereCondition]()
        if departure is not None:
            found |= severe_weather_around(at_origin, departure, policy)
        if arrival is not None:
            found |= severe_weather_around(at_destination, arrival, policy)
        return found

    def assess(self, policy: WeatherPolicy | None = None) -> Assessment:
        return assess(self.flight_case(), self.severe_weather(policy or WeatherPolicy()))

    def _outcome(self) -> Outcome:
        kind = self.data["kind"]
        if kind == "arrival_delay":
            return Arrived(timedelta(minutes=self.data["arrival_delay_minutes"]))
        if kind == "cancellation":
            departure = datetime.fromisoformat(self.data["scheduled_departure_utc"])
            return Cancelled(departure - datetime.fromisoformat(self.data["detected_at"]))
        return InProgress()
