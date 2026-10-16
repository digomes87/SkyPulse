"""Online features: the serving-side twin of ``skypulse.ml.features``.

Same contract, same semantics: calendar from the scheduled departure in UTC, weekday with
Monday as 0, great-circle distance, weather flags from the shared patterns (gust 0 when a
report has none, every weather feature missing when there is no report), and the newest
route and airline statistics computed by the feature job.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from skypulse.eu261.geography import Coordinates, great_circle_km
from skypulse.ml.feature_spec import (
    FEATURES,
    NUMERIC_FEATURES,
    PRECIPITATION_PATTERN,
    THUNDERSTORM_PATTERN,
)
from skypulse.reference.registry import airports

ROUTE_STATS = ("route_mean_arrival_delay", "route_delay_15_rate", "route_flights_30d")


@dataclass(frozen=True)
class WeatherReport:
    visibility_m: int | None
    wind_speed_kt: int | None
    wind_gust_kt: int | None
    present_weather: str | None

    def features(self, prefix: str) -> dict[str, float | None]:
        weather = self.present_weather or ""
        return {
            f"{prefix}_visibility_m": self.visibility_m,
            f"{prefix}_wind_kt": self.wind_speed_kt,
            f"{prefix}_gust_kt": self.wind_gust_kt or 0,
            f"{prefix}_thunderstorm": int(bool(re.search(THUNDERSTORM_PATTERN, weather))),
            f"{prefix}_precipitation": int(bool(re.search(PRECIPITATION_PATTERN, weather))),
        }


def _no_weather(prefix: str) -> dict[str, float | None]:
    names = ("visibility_m", "wind_kt", "gust_kt", "thunderstorm", "precipitation")
    return {f"{prefix}_{name}": None for name in names}


@dataclass(frozen=True)
class Statistics:
    """Latest route and airline statistics, keyed as the feature job wrote them."""

    routes: Mapping[tuple[str, str, str], Mapping[str, float]]
    airlines: Mapping[str, float]

    @classmethod
    def from_frame(cls, frame: pd.DataFrame) -> "Statistics":
        routes = {
            (str(row["origin_icao"]), str(row["destination_icao"]), str(row["airline_icao"])): {
                name: float(row[name]) for name in ROUTE_STATS if pd.notna(row[name])
            }
            for row in frame.to_dict("records")
        }
        rates = frame.groupby("airline_icao")["airline_delay_15_rate"].mean().dropna()
        airlines = {str(airline): float(rate) for airline, rate in rates.items()}
        return cls(routes, airlines)

    @classmethod
    def empty(cls) -> "Statistics":
        return cls({}, {})

    def for_flight(self, origin: str, destination: str, airline: str) -> dict[str, float | None]:
        route: Mapping[str, float | None] = self.routes.get((origin, destination, airline), {})
        return {name: route.get(name) for name in ROUTE_STATS} | {
            "airline_delay_15_rate": self.airlines.get(airline)
        }


def _distance(origin: str, destination: str) -> float | None:
    start, end = airports().find(origin), airports().find(destination)
    if start is None or end is None:
        return None
    return great_circle_km(
        Coordinates(start["latitude"], start["longitude"]),
        Coordinates(end["latitude"], end["longitude"]),
    )


@dataclass(frozen=True)
class FlightQuery:
    origin_icao: str
    destination_icao: str
    airline_icao: str
    scheduled_departure_utc: datetime
    origin_weather: WeatherReport | None = None
    destination_weather: WeatherReport | None = None

    def features(self, statistics: Statistics) -> pd.DataFrame:
        row: dict[str, Any] = {
            "origin_icao": self.origin_icao,
            "destination_icao": self.destination_icao,
            "airline_icao": self.airline_icao,
            "departure_hour_utc": self.scheduled_departure_utc.hour,
            "day_of_week": self.scheduled_departure_utc.weekday(),
            "distance_km": _distance(self.origin_icao, self.destination_icao),
            **(
                self.origin_weather.features("origin")
                if self.origin_weather
                else _no_weather("origin")
            ),
            **(
                self.destination_weather.features("destination")
                if self.destination_weather
                else _no_weather("destination")
            ),
            **statistics.for_flight(self.origin_icao, self.destination_icao, self.airline_icao),
        }
        frame = pd.DataFrame([{name: row.get(name) for name in FEATURES}])
        frame[list(NUMERIC_FEATURES)] = frame[list(NUMERIC_FEATURES)].astype("float64")
        return frame
