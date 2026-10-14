from pathlib import Path

import httpx
import pytest
from pyspark.sql import SparkSession

from skypulse.batch.__main__ import Job, parse_arguments
from skypulse.batch.assessments import assessments_path, latest_per_claim, recompute_assessments
from skypulse.batch.maintenance import run_maintenance
from skypulse.batch.reference import AIRPORTS_TABLE, file_reader, load_reference, reference_path
from skypulse.common.config import Settings
from skypulse.streaming.detector import detect_batch
from tests.unit.streaming.schedule_rows import at, schedule
from tests.unit.streaming.test_detector import CollectingAnnouncer, change_feed

pytestmark = pytest.mark.spark

FIXTURES = Path(__file__).parents[3] / "data" / "fixtures"


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("SKYPULSE_LAKE__ROOT_URI", tmp_path.as_uri())
    monkeypatch.setenv("SKYPULSE_INGESTION__REPLAY", "true")
    monkeypatch.setenv("SKYPULSE_INGESTION__FIXTURES_DIR", str(FIXTURES))
    return Settings()


def test_reference_load_from_fixtures(spark: SparkSession, settings: Settings) -> None:
    counts = load_reference(spark, settings, file_reader(settings))

    airports = spark.read.format("delta").load(reference_path(settings, AIRPORTS_TABLE))
    palma = airports.where("icao = 'LEPA'").first()
    assert counts[AIRPORTS_TABLE] > 800
    assert palma is not None
    assert palma["runway_headings"]


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
