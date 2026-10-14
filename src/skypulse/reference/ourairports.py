"""Parsing of the OurAirports CSV files (https://ourairports.com/data/, public domain)."""

import csv
import io
import re
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass

from skypulse.eu261.geography import EU261_COUNTRIES, CountryCode

KEPT_TYPES = frozenset({"large_airport", "medium_airport"})
_RUNWAY_NUMBER = re.compile(r"^(\d{1,2})")
DEGREES_PER_RUNWAY_NUMBER = 10


def _rows(text: str) -> Iterator[dict[str, str]]:
    return csv.DictReader(io.StringIO(text))


def _number(raw: str) -> float | None:
    try:
        return float(raw)
    except ValueError:
        return None


@dataclass(frozen=True)
class AirportRow:
    icao: str
    iata: str | None
    name: str
    airport_type: str
    country: str
    latitude: float
    longitude: float
    municipality: str | None

    def as_tuple(self) -> tuple[object, ...]:
        return (
            self.icao, self.iata, self.name, self.airport_type, self.country,
            self.latitude, self.longitude, self.municipality,
        )  # fmt: skip


def _airport(row: dict[str, str]) -> AirportRow | None:
    relevant = row["type"] in KEPT_TYPES or row["scheduled_service"] == "yes"
    if not row["icao_code"] or not relevant:
        return None
    return AirportRow(
        icao=row["icao_code"],
        iata=row["iata_code"] or None,
        name=row["name"],
        airport_type=row["type"],
        country=row["iso_country"],
        latitude=float(row["latitude_deg"]),
        longitude=float(row["longitude_deg"]),
        municipality=row["municipality"] or None,
    )


def parse_airports(text: str) -> list[AirportRow]:
    """Airports with an ICAO code that are large, medium or have scheduled service."""
    parsed = (_airport(row) for row in _rows(text))
    return [airport for airport in parsed if airport is not None]


def _heading(row: dict[str, str]) -> int | None:
    """True heading of a runway, from the data or, failing that, from its number."""
    for field in ("le_heading_degT", "he_heading_degT"):
        value = _number(row[field])
        if value is not None:
            return round(value) % 360
    match = _RUNWAY_NUMBER.match(row["le_ident"])
    if match is None:
        return None
    return int(match.group(1)) * DEGREES_PER_RUNWAY_NUMBER % 360


def parse_runway_headings(text: str) -> dict[str, list[int]]:
    """Open runways' headings per airport ident (one heading per runway, 0-359)."""
    headings: defaultdict[str, set[int]] = defaultdict(set)
    for row in _rows(text):
        heading = _heading(row)
        if row["closed"] == "1" or heading is None:
            continue
        headings[row["airport_ident"]].add(heading)
    return {ident: sorted(values) for ident, values in headings.items()}


def parse_countries(text: str) -> list[tuple[str, str, str, bool]]:
    """(code, name, continent, in EU261 scope) per country."""
    return [
        (row["code"], row["name"], row["continent"], CountryCode(row["code"]) in EU261_COUNTRIES)
        for row in _rows(text)
        if len(row["code"]) == 2 and row["code"].isalpha()
    ]
