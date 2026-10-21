"""Loads reference data into silver: airports (with runway headings), countries, airlines.

1. Land: every downloaded file is appended, untouched, to ``bronze/reference_files`` with
   its fetch time and SHA-256, so a load can be replayed or diagnosed without the source.
2. Screen: row counts are checked against the current tables (``skypulse.reference.screen``);
   a failure raises before anything in silver is written.
3. Replace: each table is overwritten; reference data is small and the latest file is the
   truth.

In replay mode the OurAirports files come from ``data/fixtures/ourairports``.
"""

import hashlib
from collections.abc import Callable, Mapping
from datetime import datetime

import httpx
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from skypulse.common.config import LakeLayer, Settings
from skypulse.common.logging import get_logger
from skypulse.reference.ourairports import parse_airports, parse_countries, parse_runway_headings
from skypulse.reference.registry import airlines
from skypulse.reference.screen import ReferenceScreenError, screen

AIRPORTS_TABLE = "reference_airports"
COUNTRIES_TABLE = "reference_countries"
AIRLINES_TABLE = "reference_airlines"
RAW_FILES_TABLE = "reference_files"
SOURCE_FILES = ("airports", "runways", "countries")
DOWNLOAD_TIMEOUT_SECONDS = 120

AIRPORT_SCHEMA = (
    "icao string, iata string, name string, airport_type string, country string, "
    "latitude double, longitude double, municipality string, runway_headings array<int>"
)
COUNTRY_SCHEMA = "code string, name string, continent string, in_eu261_scope boolean"
AIRLINE_SCHEMA = "icao string, iata string, name string, country string"
RAW_FILE_SCHEMA = (
    "name string, fetched_at timestamp, fetch_date date, sha256 string, content string"
)

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


def raw_files_path(settings: Settings) -> str:
    return f"{settings.lake.uri_for(LakeLayer.BRONZE)}/{RAW_FILES_TABLE}"


def land_raw_files(
    spark: SparkSession, settings: Settings, files: Mapping[str, str], fetched_at: datetime
) -> None:
    rows = [
        (name, fetched_at, fetched_at.date(), hashlib.sha256(content.encode()).hexdigest(), content)
        for name, content in files.items()
    ]
    frame = spark.createDataFrame(rows, RAW_FILE_SCHEMA)
    frame.write.format("delta").mode("append").partitionBy("fetch_date").save(
        raw_files_path(settings)
    )


def current_counts(spark: SparkSession, settings: Settings, tables: list[str]) -> dict[str, int]:
    paths = {table: reference_path(settings, table) for table in tables}
    return {
        table: spark.read.format("delta").load(path).count()
        for table, path in paths.items()
        if DeltaTable.isDeltaTable(spark, path)
    }


def reference_tables(spark: SparkSession, files: Mapping[str, str]) -> dict[str, DataFrame]:
    headings = parse_runway_headings(files["runways"])
    airports = [
        (*row.as_tuple(), headings.get(row.icao, [])) for row in parse_airports(files["airports"])
    ]
    carriers = [(row["icao"], row["iata"], row["name"], row["country"]) for row in airlines()]
    return {
        AIRPORTS_TABLE: spark.createDataFrame(airports, AIRPORT_SCHEMA),
        COUNTRIES_TABLE: spark.createDataFrame(parse_countries(files["countries"]), COUNTRY_SCHEMA),
        AIRLINES_TABLE: spark.createDataFrame(carriers, AIRLINE_SCHEMA),
    }


def load_reference(
    spark: SparkSession, settings: Settings, read: Reader, fetched_at: datetime
) -> dict[str, int]:
    files = {name: read(name) for name in SOURCE_FILES}
    land_raw_files(spark, settings, files, fetched_at)
    tables = reference_tables(spark, files)
    counts = {name: frame.count() for name, frame in tables.items()}
    problems = screen(
        counts, current_counts(spark, settings, list(tables)), settings.reference.min_row_ratio
    )
    if problems:
        _log.error("reference_rejected", problems=problems, **counts)
        raise ReferenceScreenError(problems)
    for name, frame in tables.items():
        frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            reference_path(settings, name)
        )
    _log.info("reference_loaded", **counts)
    return counts
