"""Batch entrypoint: ``python -m skypulse.batch reference|assessments|maintenance``."""

import argparse
from enum import StrEnum

from skypulse.batch.assessments import recompute_assessments
from skypulse.batch.maintenance import run_maintenance
from skypulse.batch.reference import file_reader, load_reference
from skypulse.common.config import Settings
from skypulse.common.logging import configure_logging
from skypulse.streaming.session import Connectors, build_session


class Job(StrEnum):
    REFERENCE = "reference"
    ASSESSMENTS = "assessments"
    MAINTENANCE = "maintenance"


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="skypulse.batch", description=__doc__)
    parser.add_argument("job", type=Job, choices=list(Job))
    parser.add_argument("--master", default=None, help="Spark master, e.g. local[*]")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    settings = Settings()
    configure_logging(settings.log_level)
    job: Job = arguments.job
    connectors = Connectors(s3a=settings.lake.root_uri is None)
    spark = build_session(f"skypulse-{job}", settings, connectors, arguments.master)
    if job is Job.REFERENCE:
        load_reference(spark, settings, file_reader(settings))
    if job is Job.ASSESSMENTS:
        recompute_assessments(spark, settings)
    if job is Job.MAINTENANCE:
        run_maintenance(spark, settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
