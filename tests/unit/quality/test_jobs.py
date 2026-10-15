from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest
from pyspark.sql import SparkSession

from skypulse.common.config import LakeLayer, Settings
from skypulse.quality import __main__ as cli
from skypulse.quality.jobs import check_gold, check_silver, check_statistics
from skypulse.quality.results import CheckResult, QualityGateError, QualityReport, Status
from skypulse.quality.store import publish_results, results_path, save_results
from tests.unit.streaming.schedule_rows import at, schedule, schedules

pytestmark = pytest.mark.spark


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("SKYPULSE_LAKE__ROOT_URI", tmp_path.as_uri())
    return Settings()


def write_schedules(spark: SparkSession, settings: Settings, *rows: tuple[object, ...]) -> None:
    path = f"{settings.lake.uri_for(LakeLayer.SILVER)}/flight_schedules"
    schedules(spark, *rows).write.format("delta").mode("overwrite").save(path)


def test_silver_checks_run_on_written_tables_and_skip_missing_ones(
    spark: SparkSession, settings: Settings
) -> None:
    write_schedules(
        spark,
        settings,
        schedule("FR1", "Arrived"),
        schedule("FR2", "Canceled", scheduled=(at(10), at(12))),
    )

    report = check_silver(spark, settings)

    suites = {result.suite for result in report}
    assert "silver.flight_schedules" in suites
    assert report.passed()
    assert any(
        result.status is Status.SKIP and result.suite == "silver.metar_observations"
        for result in report
    )


def test_statistics_report_freshness_volume_and_drift(
    spark: SparkSession, settings: Settings
) -> None:
    write_schedules(
        spark,
        settings,
        *(
            schedule(f"FR{n}", "Arrived", scheduled=(at(6 + n % 8), at(9 + n % 8)))
            for n in range(40)
        ),
    )

    first = check_statistics(spark, settings, datetime.now(UTC))
    second = check_statistics(spark, settings, datetime.now(UTC))

    checks = {result.check: result.status for result in first}
    assert checks["freshness:flight_schedules"] is Status.PASS
    assert checks["freshness:aircraft_states"] is Status.FAIL
    assert any(check.startswith("volume:") for check in checks)
    assert checks["baseline"] is Status.SKIP
    assert any(result.check.startswith("null_rate_drift:") for result in second)


def test_gold_suites_read_the_duckdb_file(tmp_path: Path) -> None:
    database = tmp_path / "gold.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute(Path(__file__).with_name("gold_quality.sql").read_text())

    assert check_gold(database).passed()


def test_results_are_appended_to_delta(spark: SparkSession, settings: Settings) -> None:
    report = QualityReport((CheckResult("silver.x", "c", Status.FAIL, 1.0, 0.0, "d"),))

    save_results(spark, settings, "silver", report, datetime.now(UTC))
    save_results(spark, settings, "silver", report, datetime.now(UTC))

    assert spark.read.format("delta").load(results_path(settings)).count() == 2


def test_results_are_pushed_when_a_gateway_is_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    pushed: list[str] = []
    monkeypatch.setattr(
        "skypulse.quality.store.push_to_gateway", lambda gateway, job, registry: pushed.append(job)
    )
    monkeypatch.setenv("SKYPULSE_METRICS__PUSHGATEWAY", "pushgateway:9091")

    assert publish_results(Settings(), "gold", QualityReport()) is True
    assert pushed == ["quality_gold"]


def test_gates_raise_and_monitors_do_not(monkeypatch: pytest.MonkeyPatch) -> None:
    failing = QualityReport((CheckResult("s", "c", Status.FAIL),))
    monkeypatch.setattr(cli, "run", lambda stage, settings, arguments: failing)

    with pytest.raises(QualityGateError):
        cli.main(["silver"])
    assert cli.main(["statistics"]) == 0
