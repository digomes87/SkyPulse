"""Spark side of ML: ``python -m skypulse.ml.pipeline backfill|features``.

``backfill`` loads synthetic history ending the day before the replayed day; ``features``
rebuilds the point-in-time feature table and the latest statistics for serving.
"""

import argparse
from datetime import UTC, datetime
from enum import StrEnum

from skypulse.common.config import Settings
from skypulse.common.logging import configure_logging, get_logger
from skypulse.ml.backfill import backfill
from skypulse.ml.features import build_feature_tables
from skypulse.ml.history import history_before
from skypulse.streaming.session import Connectors, build_session

REPLAYED_DAY = datetime(2026, 10, 14, tzinfo=UTC)
DEFAULT_HISTORY_DAYS = 120

_log = get_logger(__name__)


class Step(StrEnum):
    BACKFILL = "backfill"
    FEATURES = "features"


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="skypulse.ml.pipeline", description=__doc__)
    parser.add_argument("step", type=Step, choices=list(Step))
    parser.add_argument("--days", type=int, default=DEFAULT_HISTORY_DAYS)
    parser.add_argument("--master", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    settings = Settings()
    configure_logging(settings.log_level)
    spark = build_session(
        f"skypulse-ml-{arguments.step}",
        settings,
        Connectors(s3a=settings.lake.root_uri is None),
        arguments.master,
    )
    if arguments.step is Step.BACKFILL:
        backfill(spark, settings, history_before(REPLAYED_DAY, arguments.days))
    if arguments.step is Step.FEATURES:
        rows = build_feature_tables(spark, settings)
        _log.info("features_built", rows=rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
