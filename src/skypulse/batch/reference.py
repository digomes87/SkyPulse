"""Loads reference data into silver: airports (with runway headings), countries, airlines.

Each run replaces the tables (overwrite): reference data is small and the latest file is
the truth. In replay mode the OurAirports files come from ``data/fixtures/ourairports``.
"""

from collections.abc import Callable

import httpx
from pyspark.sql import SparkSession

from skypulse.common.config import LakeLayer, Settings
from skypulse.common.logging import get_logger
from skypulse.reference.ourairports import parse_airports, parse_countries, parse_runway_headings
from skypulse.reference.registry import airlines

AIRPORTS_TABLE = "reference_airports"
COUNTRIES_TABLE = "reference_countries"
AIRLINES_TABLE = "reference_airlines"
DOWNLOAD_TIMEOUT_SECONDS = 120

AIRPORT_SCHEMA = (
    "icao string, iata string, name string, airport_type string, country string, "
    "latitude double, longitude double, municipality string, runway_headings array<int>"
)
COUNTRY_SCHEMA = "code string, name string, continent string, in_eu261_scope boolean"
AIRLINE_SCHEMA = "icao string, iata string, name string, country string"

Reader = Callable[[str], str]

_log = get_logger(__name__)


def reference_path(settings: Settings, table: str) -> str:
    return f"{settings.lake.uri_for(LakeLayer.SILVER)}/{table}"


def file_reader(settings: Settings) -> Reader:
    """Reads an OurAirports file by name, from the fixtures in replay mode."""
    if settings.ingestion.replay:
        folder = settings.ingestion.fixtures_dir / "ourairports"
        return lambda name: (folder / f"{name}.csv").read_text()

    def download(name: str) -> str:
        url = f"{settings.reference.ourairports_base_url}/{name}.csv"
        response = httpx.get(url, timeout=DOWNLOAD_TIMEOUT_SECONDS, follow_redirects=True)
        response.raise_for_status()
        return response.text

    return download


def load_reference(spark: SparkSession, settings: Settings, read: Reader) -> dict[str, int]:
    headings = parse_runway_headings(read("runways"))
    airports = [
        (*row.as_tuple(), headings.get(row.icao, [])) for row in parse_airports(read("airports"))
    ]
    countries = parse_countries(read("countries"))
    carriers = [(row["icao"], row["iata"], row["name"], row["country"]) for row in airlines()]
    tables = {
        AIRPORTS_TABLE: spark.createDataFrame(airports, AIRPORT_SCHEMA),
        COUNTRIES_TABLE: spark.createDataFrame(countries, COUNTRY_SCHEMA),
        AIRLINES_TABLE: spark.createDataFrame(carriers, AIRLINE_SCHEMA),
    }
    for name, frame in tables.items():
        frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            reference_path(settings, name)
        )
    counts = {
        name: len(rows)
        for name, rows in (
            (AIRPORTS_TABLE, airports),
            (COUNTRIES_TABLE, countries),
            (AIRLINES_TABLE, carriers),
        )
    }
    _log.info("reference_loaded", **counts)
    return counts
