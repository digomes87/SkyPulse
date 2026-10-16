from datetime import datetime

import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession

from skypulse.quality.results import QualityGateError, QualityReport, Status
from skypulse.quality.runner import SuiteRunner
from skypulse.quality.suites import GOLD_SUITES, SILVER_SUITES

SCHEDULE_SCHEMA = (
    "flight_number string, scheduled_departure_utc timestamp, scheduled_arrival_utc timestamp, "
    "origin_icao string, destination_icao string, status string"
)


def schedules(spark: SparkSession, *rows: tuple[object, ...]) -> DataFrame:
    return spark.createDataFrame(list(rows), SCHEDULE_SCHEMA)


def at(hour: int) -> datetime:
    return datetime(2026, 10, 14, hour)  # noqa: DTZ001 - Spark session runs in UTC


@pytest.mark.spark
def test_clean_silver_schedules_pass(spark: SparkSession) -> None:
    frame = schedules(spark, ("FR1", at(9), at(11), "EIDW", "LEMD", "Arrived"))

    report = SuiteRunner().validate_spark(
        "flight_schedules", frame, SILVER_SUITES["flight_schedules"]()
    )

    assert report.passed()
    assert len(report) == len(SILVER_SUITES["flight_schedules"]())


@pytest.mark.spark
def test_broken_silver_schedules_name_the_failing_checks(spark: SparkSession) -> None:
    frame = schedules(
        spark,
        ("FR1", at(9), at(11), "EIDW", "LEMD", "Arrived"),
        ("FR1", at(9), at(8), "dublin", "LEMD", "Teleported"),
    )

    report = SuiteRunner().validate_spark(
        "flight_schedules", frame, SILVER_SUITES["flight_schedules"]()
    )

    failed = {failure.check for failure in report.failures()}
    assert "expect_column_values_to_be_in_set:status" in failed
    assert "expect_column_values_to_match_regex:origin_icao" in failed
    assert "expect_compound_columns_to_be_unique:flight_number,scheduled_departure_utc" in failed
    assert any(
        check.startswith("expect_column_pair_values_a_to_be_greater_than_b") for check in failed
    )


def gold_flights(cancelled: list[bool]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "flight_key": [f"k{index}" for index in range(len(cancelled))],
            "is_cancelled": cancelled,
            "departure_delay_minutes": [10] * len(cancelled),
            "arrival_delay_minutes": [12] * len(cancelled),
        }
    )


def test_gold_distribution_checks_catch_a_cancellation_spike() -> None:
    runner = SuiteRunner()

    normal = runner.validate_pandas(
        "fct_flight", gold_flights([False] * 9 + [True]), GOLD_SUITES["fct_flight"]()
    )
    spike = runner.validate_pandas(
        "fct_flight", gold_flights([True] * 5 + [False] * 5), GOLD_SUITES["fct_flight"]()
    )

    assert normal.passed()
    assert [failure.check for failure in spike.failures()] == [
        "expect_column_mean_to_be_between:is_cancelled"
    ]


def test_gate_error_lists_the_failures() -> None:
    report = SuiteRunner().validate_pandas(
        "fct_eu261_claim",
        pd.DataFrame({"claim_id": ["a", "a"], "amount_eur": [250, 999], "flight_key": ["k", None]}),
        GOLD_SUITES["fct_eu261_claim"](),
    )

    error = QualityGateError(report)

    assert "3 quality checks failed" in str(error)
    assert all(failure.status is Status.FAIL for failure in error.report.failures())


def test_report_merging() -> None:
    assert QualityReport().merged_with([]).passed()
