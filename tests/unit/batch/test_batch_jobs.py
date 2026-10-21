import hashlib
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from pyspark.sql import SparkSession

from skypulse.batch.__main__ import Job, parse_arguments
from skypulse.batch.assessments import assessments_path, latest_per_claim, recompute_assessments
from skypulse.batch.maintenance import run_maintenance
from skypulse.batch.reference import (
    AIRPORTS_TABLE,
    COUNTRIES_TABLE,
    file_reader,
    load_reference,
    raw_files_path,
    reference_path,
)
from skypulse.common.config import Settings
from skypulse.reference.screen import ReferenceScreenError
from skypulse.streaming.detector import detect_batch
from tests.unit.streaming.schedule_rows import at, schedule
from tests.unit.streaming.test_detector import CollectingAnnouncer, change_feed

pytestmark = pytest.mark.spark

FIXTURES = Path(__file__).parents[3] / "data" / "fixtures"
FETCHED_AT = datetime(2026, 10, 14, 6, 0, tzinfo=UTC)


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("SKYPULSE_LAKE__ROOT_URI", tmp_path.as_uri())
    monkeypatch.setenv("SKYPULSE_INGESTION__REPLAY", "true")
    monkeypatch.setenv("SKYPULSE_INGESTION__FIXTURES_DIR", str(FIXTURES))
    return Settings()


def test_reference_load_from_fixtures(spark: SparkSession, settings: Settings) -> None:
    counts = load_reference(spark, settings, file_reader(settings), FETCHED_AT)

    airports = spark.read.format("delta").load(reference_path(settings, AIRPORTS_TABLE))
    palma = airports.where("icao = 'LEPA'").first()
    assert counts[AIRPORTS_TABLE] > 800
    assert palma is not None
    assert palma["runway_headings"]


def test_reference_files_are_landed_untouched_in_bronze(
    spark: SparkSession, settings: Settings
) -> None:
    load_reference(spark, settings, file_reader(settings), FETCHED_AT)

    landed = {
        row["name"]: row
        for row in spark.read.format("delta").load(raw_files_path(settings)).collect()
    }
    countries = (FIXTURES / "ourairports" / "countries.csv").read_text()
    assert sorted(landed) == ["airports", "countries", "runways"]
    assert landed["countries"]["content"] == countries
    assert landed["countries"]["sha256"] == hashlib.sha256(countries.encode()).hexdigest()


def test_a_truncated_download_is_rejected_and_keeps_the_current_tables(
    spark: SparkSession, settings: Settings
) -> None:
    read = file_reader(settings)
    load_reference(spark, settings, read, FETCHED_AT)

    def truncated(name: str) -> str:
        content = read(name)
        return "\n".join(content.splitlines()[:50]) if name == "countries" else content

    with pytest.raises(ReferenceScreenError, match="reference_countries has 49 rows"):
        load_reference(spark, settings, truncated, FETCHED_AT)
    countries = spark.read.format("delta").load(reference_path(settings, COUNTRIES_TABLE))
    landed = spark.read.format("delta").load(raw_files_path(settings))
    assert countries.count() > 200
    assert landed.count() == 6


def test_assessments_are_recomputed_from_announced_disruptions(
    spark: SparkSession, settings: Settings
) -> None:
    late = schedule("EI105", "Arrived", route=("EIDW", "KJFK"), scheduled=(at(7, 30), at(14, 30)),
                    runway=(at(11, 40), at(18, 40)), airline="EIN")  # fmt: skip
    detect_batch(
        change_feed(spark, late, schedule("FR1234", "Canceled")), settings, CollectingAnnouncer()
    )

    assert recompute_assessments(spark, settings) == 2
    rows = {
        row["flight_number"]: row
        for row in spark.read.format("delta").load(assessments_path(settings)).collect()
    }
    assert rows["EI105"]["amount_eur"] == 600
    assert rows["FR1234"]["verdict"] == "eligible"


def test_final_outcomes_win_over_early_warnings(spark: SparkSession) -> None:
    rows = spark.createDataFrame(
        [
            ("c1", "departure_delay", "2026-10-14T12:00:00Z"),
            ("c1", "arrival_delay", "2026-10-14T10:00:00Z"),
        ],
        "claim_id string, kind string, detected_at string",
    )

    assert latest_per_claim(rows).first()["kind"] == "arrival_delay"  # type: ignore[index]


def test_maintenance_skips_missing_tables_and_optimises_existing(
    spark: SparkSession, settings: Settings
) -> None:
    detect_batch(
        change_feed(spark, schedule("FR1234", "Canceled")), settings, CollectingAnnouncer()
    )

    done = run_maintenance(spark, settings)

    assert [path.rsplit("/", 1)[-1] for path in done] == ["flight_disruptions"]


def test_cli_arguments() -> None:
    assert parse_arguments(["assessments"]).job is Job.ASSESSMENTS


def test_reader_downloads_outside_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    requested: list[str] = []

    def fake_get(url: str, **_: object) -> httpx.Response:
        requested.append(url)
        return httpx.Response(200, text="code,name", request=httpx.Request("GET", url))

    monkeypatch.setenv("SKYPULSE_INGESTION__REPLAY", "false")
    monkeypatch.setattr(httpx, "get", fake_get)

    assert file_reader(Settings())("countries") == "code,name"
    assert requested == ["https://davidmegginson.github.io/ourairports-data/countries.csv"]
