"""Reads the gold layer (DuckDB built by dbt) into plain values for the site."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb


@dataclass(frozen=True)
class Headline:
    flights: int
    disrupted: int
    eligible_claims: int
    gross_exposure_eur: float
    expected_payout_eur: float
    likely_exempt_eur: float
    aircraft_tracked: int


@dataclass(frozen=True)
class AirlineExposure:
    airline_icao: str
    airline_name: str
    flights: int
    expected_payout_eur: float
    likely_exempt_eur: float


@dataclass(frozen=True)
class AirportDelay:
    airport_icao: str
    airport_name: str
    departures: int
    avg_departure_delay_minutes: float
    on_time_rate: float | None


@dataclass(frozen=True)
class DelayBand:
    band: str
    flights: int


@dataclass(frozen=True)
class Claim:
    flight_number: str
    route: str
    disruption_kind: str
    arrival_delay_minutes: int | None
    amount_eur: float
    severe_weather: str | None


@dataclass(frozen=True)
class Position:
    longitude: float
    latitude: float
    on_ground: bool


@dataclass(frozen=True)
class Airport:
    icao: str
    longitude: float
    latitude: float


@dataclass(frozen=True)
class Gold:
    headline: Headline
    airlines: tuple[AirlineExposure, ...]
    airports: tuple[AirportDelay, ...]
    delay_bands: tuple[DelayBand, ...]
    claims: tuple[Claim, ...]
    positions: tuple[Position, ...]
    hubs: tuple[Airport, ...]


DELAY_BAND_ORDER = ("on_time", "15_to_59", "60_to_179", "180_plus", "cancelled")

_HEADLINE = """
select
    (select count(*) from main_gold.fct_flight),
    (select count(*) from main_gold.fct_flight where delay_band not in ('on_time', 'not_arrived')),
    (select count(*) from main_gold.fct_eu261_claim where is_eligible),
    (select cast(coalesce(sum(case when is_eligible then amount_eur end), 0) as double)
        from main_gold.fct_eu261_claim),
    (select cast(coalesce(sum(expected_payout_eur), 0) as double) from main_gold.fct_eu261_claim),
    (select cast(coalesce(sum(likely_exempt_eur), 0) as double) from main_gold.fct_eu261_claim),
    (select count(*) from main_gold.fct_aircraft_position)
"""
_AIRLINES = """
select airline_icao, coalesce(airline_name, airline_icao), flights,
       cast(expected_payout_eur as double), cast(likely_exempt_eur as double)
from main_gold.agg_eu261_exposure_by_airline
order by expected_payout_eur + likely_exempt_eur desc, airline_icao
"""
_AIRPORTS = """
select delays.airport_icao, coalesce(airports.airport_name, delays.airport_icao),
       sum(delays.departures),
       cast(sum(delays.avg_departure_delay_minutes * delays.departures)
            / sum(delays.departures) as double),
       cast(avg(delays.on_time_rate) as double)
from main_gold.agg_airport_daily_delays as delays
left join main_gold.dim_airport as airports using (airport_icao)
group by 1, 2
order by 4 desc
"""
_BANDS = "select delay_band, count(*) from main_gold.fct_flight group by 1"
_CLAIMS = """
select flight_number, origin_icao || ' → ' || destination_icao, disruption_kind,
       arrival_delay_minutes, cast(amount_eur as double), nullif(severe_weather, '')
from main_gold.fct_eu261_claim
where is_eligible or extraordinary_circumstances_likely
order by amount_eur desc, flight_number
"""
_POSITIONS = """
select cast(longitude as double), cast(latitude as double), on_ground
from main_gold.fct_aircraft_position
"""
_HUBS = """
select airport_icao, cast(longitude as double), cast(latitude as double)
from main_gold.dim_airport
where airport_icao in (select origin_icao from main_gold.fct_flight)
  and longitude between -30 and 45
order by airport_icao
"""


def _rows(connection: duckdb.DuckDBPyConnection, query: str) -> list[tuple[Any, ...]]:
    return connection.execute(query).fetchall()


def load_gold(database: Path) -> Gold:
    with duckdb.connect(str(database), read_only=True) as connection:
        headline_row = connection.execute(_HEADLINE).fetchone()
        if headline_row is None:
            raise ValueError(f"no gold data in {database}")
        bands = dict(_rows(connection, _BANDS))
        return Gold(
            headline=Headline(*headline_row),
            airlines=tuple(AirlineExposure(*row) for row in _rows(connection, _AIRLINES)),
            airports=tuple(AirportDelay(*row) for row in _rows(connection, _AIRPORTS)),
            delay_bands=tuple(DelayBand(band, bands.get(band, 0)) for band in DELAY_BAND_ORDER),
            claims=tuple(Claim(*row) for row in _rows(connection, _CLAIMS)),
            positions=tuple(Position(*row) for row in _rows(connection, _POSITIONS)),
            hubs=tuple(Airport(*row) for row in _rows(connection, _HUBS)),
        )
