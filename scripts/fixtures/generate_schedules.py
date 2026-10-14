"""Generates synthetic AeroDataBox FIDS snapshots for one replay day.

Usage: uv run python scripts/fixtures/generate_schedules.py

Live AeroDataBox data needs a paid key and changes every run, so demo mode uses a
deterministic synthetic day instead. Output follows the real FIDS format (withLeg=true), one
file per airport and hour, so the production parser reads it unchanged. A few scenarios are
pinned so later milestones always have something to find:

- EI105  EIDW-KJFK  long-haul, 4h10 late       -> EUR 600 band
- FR6403 LEPA-EGSS  3h35 late in a thunderstorm -> eligible but weather-exempt
- FR1234 EIDW-LEMD  cancelled on the day       -> cancellation claim
"""

import json
import math
import random
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from skypulse.ingestion.snapshots import SNAPSHOT_FORMAT

OUTPUT = Path(__file__).resolve().parents[2] / "data" / "fixtures" / "aerodatabox"
SEED = 261
DAY = datetime(2026, 10, 14, tzinfo=UTC)
FIRST_SNAPSHOT_HOUR = 5
LAST_SNAPSHOT_HOUR = 22
FIRST_DEPARTURE_HOUR = 5
LAST_DEPARTURE_HOUR = 13
RANDOM_FLIGHTS = 70
CRUISE_KMH = 780
TAXI_AND_CLIMB_MINUTES = 30
DELAY_KNOWN_BEFORE = timedelta(hours=2)
BOARDING_BEFORE = timedelta(minutes=30)
LOOKBACK = timedelta(hours=3)
LOOKAHEAD = timedelta(hours=12)


@dataclass(frozen=True)
class Airport:
    icao: str
    iata: str
    name: str
    latitude: float
    longitude: float
    timezone: str


AIRPORTS = {
    airport.icao: airport
    for airport in (
        Airport("EIDW", "DUB", "Dublin", 53.4213, -6.2701, "Europe/Dublin"),
        Airport("EGSS", "STN", "London Stansted", 51.885, 0.235, "Europe/London"),
        Airport("EGLL", "LHR", "London Heathrow", 51.4706, -0.4619, "Europe/London"),
        Airport("LEMD", "MAD", "Madrid Barajas", 40.4719, -3.5626, "Europe/Madrid"),
        Airport("LEPA", "PMI", "Palma de Mallorca", 39.5517, 2.7388, "Europe/Madrid"),
        Airport("LIRF", "FCO", "Rome Fiumicino", 41.8003, 12.2389, "Europe/Rome"),
        Airport("LFPG", "CDG", "Paris Charles de Gaulle", 49.0097, 2.5479, "Europe/Paris"),
        Airport("EDDF", "FRA", "Frankfurt", 50.0379, 8.5622, "Europe/Berlin"),
        Airport("EHAM", "AMS", "Amsterdam Schiphol", 52.3105, 4.7683, "Europe/Amsterdam"),
        Airport("LPPT", "LIS", "Lisbon", 38.7742, -9.1342, "Europe/Lisbon"),
        Airport("KJFK", "JFK", "New York JFK", 40.6413, -73.7781, "America/New_York"),
    )
}

AIRLINES = (
    ("Ryanair", "FR", "RYR", "EIDW EGSS LEMD LEPA LIRF LPPT"),
    ("Aer Lingus", "EI", "EIN", "EIDW EGLL LEMD LFPG EHAM"),
    ("Iberia", "IB", "IBE", "LEMD LEPA LPPT LFPG"),
    ("Lufthansa", "LH", "DLH", "EDDF EGLL LIRF EHAM"),
    ("Air France", "AF", "AFR", "LFPG LIRF LEMD EGLL"),
    ("KLM", "KL", "KLM", "EHAM EIDW EGLL LPPT"),
)

# (upper bound in minutes, weight); None stands for a cancellation.
DELAY_BANDS = ((15, 60), (60, 22), (180, 10), (300, 5), (None, 3))


@dataclass(frozen=True)
class Flight:
    number: str
    airline: tuple[str, str, str]
    origin: Airport
    destination: Airport
    scheduled_departure: datetime
    block: timedelta
    delay: timedelta
    cancelled: bool = False

    @property
    def scheduled_arrival(self) -> datetime:
        return self.scheduled_departure + self.block

    @property
    def actual_departure(self) -> datetime:
        return self.scheduled_departure + self.delay

    @property
    def actual_arrival(self) -> datetime:
        return self.scheduled_arrival + self.delay


def distance_km(origin: Airport, destination: Airport) -> float:
    lat1, lon1, lat2, lon2 = map(
        math.radians,
        (origin.latitude, origin.longitude, destination.latitude, destination.longitude),
    )
    inner = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return 2 * 6371.0 * math.asin(math.sqrt(inner))


def block_time(origin: Airport, destination: Airport) -> timedelta:
    minutes = distance_km(origin, destination) / CRUISE_KMH * 60 + TAXI_AND_CLIMB_MINUTES
    return timedelta(minutes=5 * round(minutes / 5))


def draw_delay(rng: random.Random) -> tuple[timedelta, bool]:
    bands = [band for band, _ in DELAY_BANDS]
    upper = rng.choices(bands, weights=[weight for _, weight in DELAY_BANDS])[0]
    if upper is None:
        return timedelta(0), True
    lower = max((band for band in bands if band is not None and band < upper), default=0)
    return timedelta(minutes=rng.randint(lower, upper - 1)), False


def random_flights(rng: random.Random) -> list[Flight]:
    flights: list[Flight] = []
    used_numbers: set[str] = set()
    while len(flights) < RANDOM_FLIGHTS:
        name, iata, icao, network = rng.choice(AIRLINES)
        origin_code, destination_code = rng.sample(network.split(), 2)
        number = f"{iata}{rng.randint(100, 9899)}"
        if number in used_numbers:
            continue
        used_numbers.add(number)
        origin, destination = AIRPORTS[origin_code], AIRPORTS[destination_code]
        departure = DAY + timedelta(
            hours=rng.randint(FIRST_DEPARTURE_HOUR, LAST_DEPARTURE_HOUR),
            minutes=5 * rng.randint(0, 11),
        )
        delay, cancelled = draw_delay(rng)
        flights.append(
            Flight(
                number,
                (name, iata, icao),
                origin,
                destination,
                departure,
                block_time(origin, destination),
                delay,
                cancelled,
            )
        )
    return flights


def pinned_flights() -> list[Flight]:
    def flight(number: str, airline: tuple[str, str, str], route: str, departure: str) -> Flight:
        origin, destination = (AIRPORTS[code] for code in route.split("-"))
        hours, minutes = (int(part) for part in departure.split(":"))
        scheduled = DAY + timedelta(hours=hours, minutes=minutes)
        return Flight(
            number,
            airline,
            origin,
            destination,
            scheduled,
            block_time(origin, destination),
            timedelta(0),
        )

    aer_lingus = ("Aer Lingus", "EI", "EIN")
    ryanair = ("Ryanair", "FR", "RYR")
    return [
        replace(flight("EI105", aer_lingus, "EIDW-KJFK", "07:30"), delay=timedelta(minutes=250)),
        replace(flight("FR6403", ryanair, "LEPA-EGSS", "12:30"), delay=timedelta(minutes=215)),
        replace(flight("FR1234", ryanair, "EIDW-LEMD", "09:15"), cancelled=True),
    ]


def moment(instant: datetime, airport: Airport) -> dict[str, str]:
    local = instant.astimezone(ZoneInfo(airport.timezone))
    return {"utc": instant.strftime("%Y-%m-%d %H:%MZ"), "local": local.strftime("%Y-%m-%d %H:%M%z")}


def status(flight: Flight, at: datetime, departing: bool) -> str:
    if flight.cancelled:
        return "Canceled" if at >= flight.scheduled_departure - DELAY_KNOWN_BEFORE else "Expected"
    if at >= flight.actual_arrival:
        return "Arrived"
    if at >= flight.actual_departure:
        return "Departed" if departing else "EnRoute"
    if at >= flight.actual_departure - BOARDING_BEFORE:
        return "Boarding" if departing else "Expected"
    delay_known = at >= flight.scheduled_departure - DELAY_KNOWN_BEFORE
    return "Delayed" if delay_known and flight.delay >= timedelta(minutes=15) else "Expected"


def leg(flight: Flight, at: datetime, departing_leg: bool) -> dict[str, object]:
    airport = flight.origin if departing_leg else flight.destination
    scheduled = flight.scheduled_departure if departing_leg else flight.scheduled_arrival
    actual = flight.actual_departure if departing_leg else flight.actual_arrival
    body: dict[str, object] = {
        "scheduledTime": moment(scheduled, airport),
        "quality": ["Basic", "Live"],
    }
    delay_known = at >= flight.scheduled_departure - DELAY_KNOWN_BEFORE
    if delay_known and not flight.cancelled:
        body["revisedTime"] = moment(actual, airport)
    if at >= actual and not flight.cancelled:
        body["runwayTime"] = moment(actual, airport)
    return body


def fids_item(flight: Flight, at: datetime, requested: Airport) -> dict[str, object]:
    departing = flight.origin == requested
    departure, arrival = leg(flight, at, True), leg(flight, at, False)
    other = flight.destination if departing else flight.origin
    (arrival if departing else departure)["airport"] = {
        "icao": other.icao,
        "iata": other.iata,
        "name": other.name,
    }
    name, iata, icao = flight.airline
    return {
        "departure": departure,
        "arrival": arrival,
        "number": f"{flight.number[:2]} {flight.number[2:]}",
        "callSign": f"{icao}{flight.number[2:]}",
        "status": status(flight, at, departing),
        "codeshareStatus": "IsOperator",
        "isCargo": False,
        "airline": {"name": name, "iata": iata, "icao": icao},
    }


def snapshot(
    flights: list[Flight], airport: Airport, at: datetime
) -> dict[str, list[dict[str, object]]]:
    def in_view(instant: datetime) -> bool:
        return at - LOOKBACK <= instant <= at + LOOKAHEAD

    departures = [
        f
        for f in flights
        if f.origin == airport and (in_view(f.scheduled_departure) or in_view(f.actual_departure))
    ]
    arrivals = [
        f
        for f in flights
        if f.destination == airport and (in_view(f.scheduled_arrival) or in_view(f.actual_arrival))
    ]
    return {
        "departures": [fids_item(f, at, airport) for f in departures],
        "arrivals": [fids_item(f, at, airport) for f in arrivals],
    }


def main() -> None:
    rng = random.Random(SEED)
    flights = sorted(pinned_flights() + random_flights(rng), key=lambda f: f.scheduled_departure)
    for airport in AIRPORTS.values():
        for hour in range(FIRST_SNAPSHOT_HOUR, LAST_SNAPSHOT_HOUR + 1):
            at = DAY + timedelta(hours=hour)
            path = OUTPUT / airport.icao / f"{at.strftime(SNAPSHOT_FORMAT)}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(snapshot(flights, airport, at), separators=(",", ":")) + "\n"
            )
    print(f"{len(flights)} flights, {len(AIRPORTS)} airports -> {OUTPUT}")


if __name__ == "__main__":
    main()
