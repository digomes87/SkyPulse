"""Quality entrypoint: ``python -m skypulse.quality silver|gold|statistics``.

``silver`` and ``gold`` are gates: any failed expectation exits non-zero so the
orchestrator stops downstream tasks. ``statistics`` (freshness, volume, null drift) only
reports: those are monitors with alerts, not gates.
"""

import argparse
import os
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from skypulse.common.config import Settings
from skypulse.common.logging import configure_logging, get_logger
from skypulse.quality.jobs import check_gold, check_silver, check_statistics
from skypulse.quality.results import QualityGateError, QualityReport
from skypulse.quality.store import publish_results, save_results
from skypulse.streaming.session import Connectors, build_session

DEFAULT_GOLD_DATABASE = "dbt/target/skypulse.duckdb"

_log = get_logger(__name__)


class Stage(StrEnum):
    SILVER = "silver"
    GOLD = "gold"
    STATISTICS = "statistics"

    def is_gate(self) -> bool:
        return self is not Stage.STATISTICS


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="skypulse.quality", description=__doc__)
    parser.add_argument("stage", type=Stage, choices=list(Stage))
    parser.add_argument("--master", default=None)
    parser.add_argument(
        "--gold-database",
        type=Path,
        default=Path(os.environ.get("SKYPULSE_DUCKDB_PATH", DEFAULT_GOLD_DATABASE)),
    )
    return parser.parse_args(argv)


def run(stage: Stage, settings: Settings, arguments: argparse.Namespace) -> QualityReport:
    spark = build_session(
        f"skypulse-quality-{stage}",
        settings,
        Connectors(s3a=settings.lake.root_uri is None),
        arguments.master,
    )
    now = datetime.now(UTC)
    reports = {
        Stage.SILVER: lambda: check_silver(spark, settings),
        Stage.GOLD: lambda: check_gold(arguments.gold_database),
        Stage.STATISTICS: lambda: check_statistics(spark, settings, now),
    }
    report = reports[stage]()
    save_results(spark, settings, str(stage), report, now)
    publish_results(settings, str(stage), report)
    return report


def main(argv: list[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    settings = Settings()
    configure_logging(settings.log_level)
    stage: Stage = arguments.stage
    report = run(stage, settings, arguments)
    failures = report.failures()
    _log.info("quality_completed", stage=str(stage), checks=len(report), failed=len(failures))
    for failure in failures:
        _log.warning(
            "quality_check_failed",
            suite=failure.suite,
            check=failure.check,
            details=failure.details,
        )
    if failures and stage.is_gate():
        raise QualityGateError(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
