"""Quality stages run by Airflow after silver and after the gold build."""

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions

from skypulse.common.config import LakeLayer, Settings
from skypulse.observability.lake import LakeTable, last_commit
from skypulse.quality.reconciliation import (
    GOLD_FACTS,
    bronze_to_quarantine,
    bronze_to_silver,
    recent,
    silver_to_gold,
)
from skypulse.quality.results import CheckResult, QualityReport, Status
from skypulse.quality.runner import SuiteRunner
from skypulse.quality.statistics import (
    FRESHNESS_SLAS,
    HourlyCount,
    evaluate_freshness,
    null_rate_drift,
    volume_anomaly,
)
from skypulse.quality.suites import GOLD_SUITES, SILVER_SUITES
from skypulse.streaming.bronze import bronze_path
from skypulse.streaming.silver import SILVER_TABLES, quarantine_path

GOLD_SCHEMA = "main_gold"
NULL_BASELINE_TABLE = "quality_null_baseline"
DRIFT_TABLE = "flight_schedules"


def _silver(settings: Settings, table: str) -> str:
    return f"{settings.lake.uri_for(LakeLayer.SILVER)}/{table}"


def _read(spark: SparkSession, path: str) -> DataFrame | None:
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return spark.read.format("delta").load(path)


def check_silver(spark: SparkSession, settings: Settings) -> QualityReport:
    runner = SuiteRunner()
    report = QualityReport()
    for table, suite in SILVER_SUITES.items():
        frame = _read(spark, _silver(settings, table))
        if frame is None:
            report = report.merged_with(
                [
                    CheckResult(
                        f"silver.{table}", "table_exists", Status.SKIP, details="not written yet"
                    )
                ]
            )
            continue
        report = report.merged_with(list(runner.validate_spark(f"silver.{table}", frame, suite())))
    return report


def check_gold(database: Path) -> QualityReport:
    runner = SuiteRunner()
    report = QualityReport()
    with duckdb.connect(str(database), read_only=True) as connection:
        for table, suite in GOLD_SUITES.items():
            frame = connection.table(f"{GOLD_SCHEMA}.{table}").df()
            report = report.merged_with(
                list(runner.validate_pandas(f"gold.{table}", frame, suite()))
            )
    return report


def _hourly_counts(schedules: DataFrame) -> list[HourlyCount]:
    hours = schedules.select(functions.date_trunc("hour", "scheduled_departure_utc").alias("hour"))
    counted = Counter(row["hour"].replace(tzinfo=UTC) for row in hours.collect())
    return [HourlyCount(hour, count) for hour, count in sorted(counted.items())]


def _null_rates(frame: DataFrame) -> dict[str, float]:
    total = frame.count()
    if total == 0:
        return {}
    nulls = frame.select(
        [
            functions.sum(functions.col(column).isNull().cast("int")).alias(column)
            for column in frame.columns
        ]
    ).first()
    return {column: (nulls[column] or 0) / total for column in frame.columns} if nulls else {}


def _drift(spark: SparkSession, settings: Settings, schedules: DataFrame) -> list[CheckResult]:
    current = _null_rates(schedules)
    baseline_path = _silver(settings, NULL_BASELINE_TABLE)
    stored = _read(spark, baseline_path)
    baseline: Mapping[str, float] = (
        {row["column"]: row["null_rate"] for row in stored.collect()} if stored else {}
    )
    spark.createDataFrame(list(current.items()), "column string, null_rate double").write.format(
        "delta"
    ).mode("overwrite").save(baseline_path)
    if not baseline:
        return [
            CheckResult(
                f"null_drift:{DRIFT_TABLE}", "baseline", Status.SKIP, details="baseline recorded"
            )
        ]
    return null_rate_drift(DRIFT_TABLE, baseline, current)


def check_statistics(spark: SparkSession, settings: Settings, now: datetime) -> QualityReport:
    freshness = [
        evaluate_freshness(sla, last_commit(LakeTable(LakeLayer.SILVER, sla.table), settings), now)
        for sla in FRESHNESS_SLAS
    ]
    report = QualityReport().merged_with(freshness)
    schedules = _read(spark, _silver(settings, DRIFT_TABLE))
    if schedules is None:
        return report
    counts = _hourly_counts(schedules)
    if counts:
        report = report.merged_with([volume_anomaly(counts[:-1], counts[-1]).result])
    return report.merged_with(_drift(spark, settings, schedules))


def _bronze_reconciliation(
    spark: SparkSession, settings: Settings, now: datetime
) -> list[CheckResult]:
    bronze = _read(spark, bronze_path(settings))
    if bronze is None:
        return [CheckResult("reconciliation", "bronze", Status.SKIP, details="no bronze yet")]
    window = recent(bronze, now).cache()
    quarantine = _read(spark, quarantine_path(settings))
    results = [
        check
        for table in SILVER_TABLES
        for check in (
            bronze_to_silver(window, _read(spark, _silver(settings, table.name)), table),
            bronze_to_quarantine(window, quarantine, table),
        )
    ]
    window.unpersist()
    return results


def _gold_count(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    counted = connection.table(f"{GOLD_SCHEMA}.{table}").count("*").fetchone()
    return 0 if counted is None else int(counted[0])


def _gold_reconciliation(
    spark: SparkSession, settings: Settings, database: Path
) -> list[CheckResult]:
    with duckdb.connect(str(database), read_only=True) as connection:
        gold_rows = {fact.fact: _gold_count(connection, fact.fact) for fact in GOLD_FACTS}
    results = []
    for fact in GOLD_FACTS:
        silver = _read(spark, _silver(settings, fact.silver_table))
        silver_rows = 0 if silver is None else silver.count()
        results.append(silver_to_gold(fact, silver_rows, gold_rows[fact.fact]))
    return results


def check_reconciliation(
    spark: SparkSession, settings: Settings, database: Path, now: datetime
) -> QualityReport:
    return (
        QualityReport()
        .merged_with(_bronze_reconciliation(spark, settings, now))
        .merged_with(_gold_reconciliation(spark, settings, database))
    )
