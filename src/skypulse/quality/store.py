"""Keeps quality results: appended to a Delta table and published as Prometheus metrics."""

from datetime import datetime

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pyspark.sql import SparkSession

from skypulse.common.clock import utc_iso
from skypulse.common.config import LakeLayer, Settings
from skypulse.quality.results import QualityReport, Status

RESULTS_TABLE = "quality_results"
RESULTS_SCHEMA = (
    "run_at string, stage string, suite string, check string, status string, "
    "observed double, threshold double, details string"
)
QUALITY_METRICS = ("skypulse_quality_check_status", "skypulse_quality_checks_failed")
STATUS_VALUE = {Status.PASS: 1.0, Status.FAIL: 0.0, Status.SKIP: -1.0}


def results_path(settings: Settings) -> str:
    return f"{settings.lake.uri_for(LakeLayer.SILVER)}/{RESULTS_TABLE}"


def save_results(
    spark: SparkSession, settings: Settings, stage: str, report: QualityReport, run_at: datetime
) -> None:
    rows = [
        (utc_iso(run_at), stage, result.suite, result.check, str(result.status),
         result.observed, result.threshold, result.details)
        for result in report
    ]  # fmt: skip
    if rows:
        frame = spark.createDataFrame(rows, RESULTS_SCHEMA)
        frame.write.format("delta").mode("append").save(results_path(settings))


def publish_results(settings: Settings, stage: str, report: QualityReport) -> bool:
    """One gauge per check: 1 pass, 0 fail, -1 skipped; pushed under job quality_<stage>."""
    if settings.metrics.pushgateway is None:
        return False
    registry = CollectorRegistry()
    status = Gauge(
        "skypulse_quality_check_status",
        "1 pass, 0 fail, -1 skip",
        ["suite", "check"],
        registry=registry,
    )
    failed = Gauge(
        "skypulse_quality_checks_failed", "Failed checks in the last run", registry=registry
    )
    for result in report:
        status.labels(result.suite, result.check).set(STATUS_VALUE[result.status])
    failed.set(len(report.failures()))
    push_to_gateway(settings.metrics.pushgateway, job=f"quality_{stage}", registry=registry)
    return True
