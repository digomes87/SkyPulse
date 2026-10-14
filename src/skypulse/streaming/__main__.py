"""Streaming entrypoint: ``python -m skypulse.streaming bronze|silver [--available-now]``."""

import argparse
from enum import StrEnum

from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from skypulse.common.config import Settings
from skypulse.common.logging import configure_logging, get_logger
from skypulse.streaming.bronze import start_bronze
from skypulse.streaming.jobs import start_quarantine, start_silver
from skypulse.streaming.session import Connectors, build_session
from skypulse.streaming.silver import SILVER_TABLES
from skypulse.streaming.triggers import Trigger

_log = get_logger(__name__)


class Stage(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"

    def connectors(self, settings: Settings) -> Connectors:
        uses_s3 = settings.lake.root_uri is None
        return Connectors(kafka=self is Stage.BRONZE, s3a=uses_s3)


def start_stage(
    stage: Stage, spark: SparkSession, settings: Settings, trigger: Trigger
) -> list[StreamingQuery]:
    if stage is Stage.BRONZE:
        return [start_bronze(spark, settings, trigger)]
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


def main(argv: list[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    settings = Settings()
    configure_logging(settings.log_level)
    stage: Stage = arguments.stage
    spark = build_session(
        f"skypulse-{stage}", settings, stage.connectors(settings), arguments.master
    )
    queries = start_stage(stage, spark, settings, Trigger(available_now=arguments.available_now))
    _log.info("streaming_started", stage=str(stage), queries=[query.name for query in queries])
    for query in queries:
        query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
