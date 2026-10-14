"""Prints row counts per lake table: ``python -m skypulse.streaming.summary``."""

import json

from pyspark.sql import SparkSession

from skypulse.common.config import Settings
from skypulse.streaming.bronze import bronze_path
from skypulse.streaming.session import Connectors, build_session
from skypulse.streaming.silver import SILVER_TABLES, quarantine_path, silver_path


def table_counts(spark: SparkSession, settings: Settings) -> dict[str, int]:
    paths = {
        "bronze.raw_events": bronze_path(settings),
        **{f"silver.{table.name}": silver_path(settings, table) for table in SILVER_TABLES},
        "silver.quarantine": quarantine_path(settings),
    }
    return {name: _count(spark, path) for name, path in paths.items()}


def _count(spark: SparkSession, path: str) -> int:
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:  # an empty layer has no Delta log yet
        return 0


def main() -> None:
    settings = Settings()
    spark = build_session(
        "skypulse-summary", settings, Connectors(s3a=settings.lake.root_uri is None)
    )
    print(json.dumps(table_counts(spark, settings), indent=2))  # noqa: T201
    spark.stop()


if __name__ == "__main__":
    main()
