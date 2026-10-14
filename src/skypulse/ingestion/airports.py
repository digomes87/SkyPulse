"""Airports SkyPulse polls for schedules and weather.

The list is package data for now; it moves to the reference-data tables in M5 (TD-001).
"""

import json
from collections.abc import Iterator
from dataclasses import dataclass
from importlib import resources
from zoneinfo import ZoneInfo

from skypulse.common.aviation import AirportIcao


@dataclass(frozen=True)
class MonitoredAirport:
    icao: AirportIcao
    timezone: ZoneInfo


@dataclass(frozen=True)
class MonitoredAirports:
    airports: tuple[MonitoredAirport, ...]

    @classmethod
    def load(cls) -> "MonitoredAirports":
        raw = resources.files("skypulse.ingestion").joinpath("monitored_airports.json")
        entries = json.loads(raw.read_text())
        return cls(
            tuple(
                MonitoredAirport(AirportIcao(entry["icao"]), ZoneInfo(entry["timezone"]))
                for entry in entries
            )
        )

    def codes(self) -> list[AirportIcao]:
        return [airport.icao for airport in self.airports]

    def __iter__(self) -> Iterator[MonitoredAirport]:
        return iter(self.airports)

    def __len__(self) -> int:
        return len(self.airports)
