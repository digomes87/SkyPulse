"""Streaming entrypoint: ``python -m skypulse.streaming bronze|silver [--available-now]``."""

import argparse
from enum import StrEnum

from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from skypulse.common.config import Settings
from skypulse.common.logging import configure_logging, get_logger
from skypulse.observability.metrics import start_metrics_server
from skypulse.observability.streaming import MetricsListener, push_metrics
from skypulse.streaming.announcements import sns_announcer
from skypulse.streaming.bronze import bronze_path, start_bronze
from skypulse.streaming.detector import start_detector
from skypulse.streaming.jobs import (
    ensure_quarantine_table,
    start_quarantine,
    start_silver,
    wait_for_table,
)
from skypulse.streaming.session import Connectors, build_session
from skypulse.streaming.silver import SILVER_TABLES
from skypulse.streaming.triggers import Trigger

_log = get_logger(__name__)


class Stage(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    DETECTOR = "detector"

    def connectors(self, settings: Settings) -> Connectors:
        uses_s3 = settings.lake.root_uri is None
        return Connectors(kafka=self is Stage.BRONZE, s3a=uses_s3)


def start_stage(
    stage: Stage, spark: SparkSession, settings: Settings, trigger: Trigger
) -> list[StreamingQuery]:
    if stage is Stage.BRONZE:
        return [start_bronze(spark, settings, trigger)]
    if stage is Stage.SILVER:
        wait_for_table(spark, bronze_path(settings))
    if stage is Stage.DETECTOR:
        return [start_detector(spark, settings, sns_announcer(settings), trigger)]
    ensure_quarantine_table(spark, settings, SILVER_TABLES[0])
    silver = [start_silver(spark, settings, table, trigger) for table in SILVER_TABLES]
    quarantine = [start_quarantine(spark, settings, table, trigger) for table in SILVER_TABLES]
    return silver + quarantine


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="skypulse.streaming", description=__doc__)
    parser.add_argument("stage", type=Stage, choices=list(Stage))
    parser.add_argument(
        "--available-now", action="store_true", help="process what is pending, then stop"
    )
    parser.add_argument("--master", default=None, help="Spark master, e.g. local[*]")
    return parser.parse_args(argv)


def instrument(spark: SparkSession, settings: Settings) -> None:
    """Progress metrics for every query, scraped on the metrics port when one is set."""
    spark.streams.addListener(MetricsListener())
    start_metrics_server(settings)


def main(argv: list[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    settings = Settings()
    configure_logging(settings.log_level)
    stage: Stage = arguments.stage
    spark = build_session(
        f"skypulse-{stage}", settings, stage.connectors(settings), arguments.master
    )
    instrument(spark, settings)
    queries = start_stage(stage, spark, settings, Trigger(available_now=arguments.available_now))
    _log.info("streaming_started", stage=str(stage), queries=[query.name for query in queries])
    for query in queries:
        query.awaitTermination()
    push_metrics(settings, f"streaming_{stage}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
