"""A deterministic synthetic history of flights and airport weather for model training.

Live schedule data needs a paid provider (TD-003), and one replayed day is far too little
to learn from. This generates months of flights and hourly METAR with delay causes that
are known on purpose, so a model has real signal to find and the evaluation means
something:

- later departures inherit the day's knock-on delays,
- some airlines and routes run late more often than others,
- Fridays and Sundays are busier,
- thunderstorms, fog and strong gusts at the origin (and storms at the destination) add
  large delays and cause most cancellations.

The output is shaped like the provider payloads (flight schedules and AWC METAR), so it
goes through the same contracts, events and silver tables as live data.
"""

import random
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

from skypulse.common.clock import utc_iso
from skypulse.eu261.geography import Coordinates, great_circle_km
from skypulse.reference.registry import airports

SEED = 1014
FLIGHTS_PER_DAY = 60
AIRLINES = (
    ("Ryanair", "FR", "RYR", 4.0),
    ("Aer Lingus", "EI", "EIN", 2.0),
    ("Iberia", "IB", "IBE", 1.0),
    ("Lufthansa", "LH", "DLH", -2.0),
    ("Air France", "AF", "AFR", 3.0),
    ("KLM", "KL", "KLM", -1.0),
)
CRUISE_KMH = 780
WEEKDAY_EFFECT = {4: 6.0, 6: 4.0}
VISIBILITY_UNLIMITED = 9999


class Regime(Enum):
    """The day's weather pattern at one airport, with its probability."""

    CLEAR = 0.70
    RAIN = 0.15
    MORNING_FOG = 0.08
    AFTERNOON_STORMS = 0.07


@dataclass(frozen=True)
class Weather:
    visibility_m: int
    wind_kt: int
    gust_kt: int | None
    present_weather: str | None

    def thunderstorm(self) -> bool:
        return "TS" in (self.present_weather or "")

    def delay_minutes(self) -> float:
        storm = 70.0 if self.thunderstorm() else 0.0
        fog = 45.0 if self.visibility_m < 800 else 0.0
        rain = 10.0 if self.present_weather and "RA" in self.present_weather else 0.0
        gusts = 25.0 if (self.gust_kt or 0) > 30 else 0.0
        return storm + fog + rain + gusts


def weather_for(regime: Regime, hour: int, rng: random.Random) -> Weather:
    wind = rng.randint(4, 16)
    if regime is Regime.MORNING_FOG and 4 <= hour <= 9:
        return Weather(rng.choice([150, 300, 500, 700]), rng.randint(0, 4), None, "FG")
    if regime is Regime.AFTERNOON_STORMS and 13 <= hour <= 19:
        return Weather(
            rng.choice([1500, 2500, 4000]), rng.randint(18, 30), rng.randint(32, 50), "+TSRA"
        )
    if regime is Regime.RAIN:
        return Weather(rng.choice([4000, 6000, 8000]), wind, None, rng.choice(["-RA", "RA"]))
    return Weather(VISIBILITY_UNLIMITED, wind, None, None)


@dataclass(frozen=True)
class Route:
    origin: str
    destination: str
    effect: float
    block: timedelta


def _distance_km(origin: Mapping[str, Any], destination: Mapping[str, Any]) -> float:
    return great_circle_km(
        Coordinates(origin["latitude"], origin["longitude"]),
        Coordinates(destination["latitude"], destination["longitude"]),
    )


@dataclass(frozen=True)
class Timetable:
    """The same flights every day, as airlines publish them for a season."""

    flights: tuple[tuple[str, tuple[str, str, str, float], Route, int], ...]

    @classmethod
    def generate(cls, rng: random.Random) -> "Timetable":
        reference = {row["icao"]: row for row in airports() if row["icao"] != "KJFK"}
        codes = sorted(reference)
        flights = []
        for index in range(FLIGHTS_PER_DAY):
            airline = rng.choice(AIRLINES)
            origin, destination = rng.sample(codes, 2)
            distance = _distance_km(reference[origin], reference[destination])
            block = timedelta(minutes=5 * round((distance / CRUISE_KMH * 60 + 30) / 5))
            route = Route(origin, destination, rng.gauss(0, 6), block)
            number = f"{airline[1]}{1000 + index}"
            minute_of_day = rng.randint(5 * 60, 21 * 60) // 5 * 5
            flights.append((number, airline, route, minute_of_day))
        return cls(tuple(sorted(flights, key=lambda flight: flight[3])))

    def origins(self) -> list[str]:
        return sorted(
            {route.origin for _, _, route, _ in self.flights}
            | {route.destination for _, _, route, _ in self.flights}
        )


def _moment(instant: datetime | None) -> str | None:
    return utc_iso(instant) if instant is not None else None


@dataclass(frozen=True)
class SyntheticHistory:
    start: datetime
    days: int
    seed: int = SEED

    def _regimes(self, rng: random.Random, stations: list[str]) -> dict[tuple[str, int], Regime]:
        regimes = list(Regime)
        weights = [regime.value for regime in regimes]
        return {
            (station, day): rng.choices(regimes, weights)[0]
            for station in stations
            for day in range(self.days)
        }

    def generate(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """(flight schedule records in the contract's data shape, METAR payloads in AWC shape)."""
        rng = random.Random(self.seed)  # noqa: S311 - synthetic data, not security
        timetable = Timetable.generate(rng)
        stations = timetable.origins()
        regimes = self._regimes(rng, stations)
        weather = {
            (station, day, hour): weather_for(regimes[(station, day)], hour, rng)
            for station in stations
            for day in range(self.days)
            for hour in range(24)
        }
        metar = [
            self._metar(station, day, hour, observed)
            for (station, day, hour), observed in weather.items()
        ]
        flights = list(self._flights(timetable, weather, rng))
        return flights, metar

    def _metar(self, station: str, day: int, hour: int, observed: Weather) -> dict[str, Any]:
        moment = self.start + timedelta(days=day, hours=hour, minutes=-10)
        return {
            "icaoId": station,
            "obsTime": int(moment.timestamp()),
            "metarType": "METAR",
            "wdir": 220,
            "wspd": observed.wind_kt,
            "wgst": observed.gust_kt,
            "visib": "6+"
            if observed.visibility_m >= VISIBILITY_UNLIMITED
            else round(observed.visibility_m / 1609.344, 2),
            "wxString": observed.present_weather,
            "temp": 14,
            "dewp": 10,
            "altim": 1015,
            "rawOb": f"METAR {station} SYNTHETIC",
            "fltCat": "VFR" if observed.visibility_m >= 5000 else "IFR",
        }

    def _flights(
        self, timetable: Timetable, weather: dict[tuple[str, int, int], Weather], rng: random.Random
    ) -> Iterator[dict[str, Any]]:
        for day in range(self.days):
            for number, airline, route, minute_of_day in timetable.flights:
                departure = self.start + timedelta(days=day, minutes=minute_of_day)
                arrival = departure + route.block
                origin = weather[(route.origin, day, departure.hour)]
                destination = weather.get(
                    (route.destination, (arrival - self.start).days, arrival.hour), origin
                )
                flight = ScheduledFlight(number, airline, route, departure)
                yield flight.outcome(Conditions(origin, destination), rng)


@dataclass(frozen=True)
class Conditions:
    origin: Weather
    destination: Weather

    def delay_minutes(self) -> float:
        storm_at_destination = 40.0 if self.destination.thunderstorm() else 0.0
        return self.origin.delay_minutes() + storm_at_destination

    def cancellation_rate(self) -> float:
        return 0.15 if self.origin.thunderstorm() else 0.005


@dataclass(frozen=True)
class ScheduledFlight:
    number: str
    airline: tuple[str, str, str, float]
    route: Route
    departure: datetime

    def base_delay(self) -> float:
        knock_on = 0.9 * max(self.departure.hour - 6, 0)
        weekday = WEEKDAY_EFFECT.get(self.departure.weekday(), 0.0)
        return self.airline[3] + self.route.effect + knock_on + weekday

    def outcome(self, conditions: Conditions, rng: random.Random) -> dict[str, Any]:
        disruption = rng.expovariate(1 / 150) if rng.random() < 0.03 else 0.0
        noise = rng.expovariate(1 / 12) - 8
        delay = max(-15.0, self.base_delay() + conditions.delay_minutes() + noise + disruption)
        cancelled = rng.random() < conditions.cancellation_rate()
        scheduled_arrival = self.departure + self.route.block
        actual_departure = None if cancelled else self.departure + timedelta(minutes=round(delay))
        actual_arrival = (
            None
            if cancelled
            else scheduled_arrival + timedelta(minutes=round(delay + rng.gauss(0, 5)))
        )
        return self._record(scheduled_arrival, actual_departure, actual_arrival, cancelled)

    def _record(
        self,
        scheduled_arrival: datetime,
        actual_departure: datetime | None,
        actual_arrival: datetime | None,
        cancelled: bool,
    ) -> dict[str, Any]:
        name, iata, icao, _ = self.airline
        final = actual_arrival or self.departure
        return {
            "flight_number": self.number,
            "callsign": f"{icao}{self.number[2:]}",
            "airline_name": name,
            "airline_iata": iata,
            "airline_icao": icao,
            "origin_icao": self.route.origin,
            "destination_icao": self.route.destination,
            "scheduled_departure_utc": utc_iso(self.departure),
            "scheduled_arrival_utc": utc_iso(scheduled_arrival),
            "revised_departure_utc": _moment(actual_departure),
            "revised_arrival_utc": _moment(actual_arrival),
            "runway_departure_utc": _moment(actual_departure),
            "runway_arrival_utc": _moment(actual_arrival),
            "status": "Canceled" if cancelled else "Arrived",
            "is_cargo": False,
            "observed_at": utc_iso(final + timedelta(minutes=10)),
        }


def history_before(day: datetime, days: int) -> SyntheticHistory:
    """History that ends the day before ``day`` (the replayed day), at midnight UTC."""
    start = datetime(day.year, day.month, day.day, tzinfo=UTC) - timedelta(days=days)
    return SyntheticHistory(start, days)
