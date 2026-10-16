from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st

from skypulse.quality.results import Status
from skypulse.quality.statistics import (
    FreshnessSla,
    HourlyCount,
    evaluate_freshness,
    null_rate_drift,
    volume_anomaly,
)

MONDAY_9 = datetime(2026, 10, 12, 9, tzinfo=UTC)


def hourly(
    start: datetime, counts: list[int], step: timedelta = timedelta(hours=1)
) -> list[HourlyCount]:
    return [HourlyCount(start + index * step, count) for index, count in enumerate(counts)]


def test_seasonal_baseline_uses_the_same_hour_of_week() -> None:
    history = hourly(MONDAY_9 - timedelta(weeks=4), [100, 104, 96, 100], step=timedelta(weeks=1))

    check = volume_anomaly(history, HourlyCount(MONDAY_9, 40))

    assert check.method == "seasonal"
    assert check.status is Status.FAIL
    assert check.expected == pytest.approx(100)


def test_trailing_window_when_there_is_no_seasonal_history() -> None:
    history = hourly(MONDAY_9 - timedelta(hours=8), [50, 52, 48, 51, 49, 50, 53, 47])

    check = volume_anomaly(history, HourlyCount(MONDAY_9, 51))

    assert check.method == "trailing"
    assert check.status is Status.PASS


def test_too_little_history_is_skipped_not_failed() -> None:
    check = volume_anomaly(hourly(MONDAY_9 - timedelta(hours=2), [5, 6]), HourlyCount(MONDAY_9, 90))

    assert check.status is Status.SKIP


def test_flat_history_uses_a_poisson_noise_floor() -> None:
    history = hourly(MONDAY_9 - timedelta(hours=6), [20] * 6)

    assert volume_anomaly(history, HourlyCount(MONDAY_9, 24)).status is Status.PASS
    assert volume_anomaly(history, HourlyCount(MONDAY_9, 45)).status is Status.FAIL


@given(st.lists(st.integers(min_value=10, max_value=500), min_size=6, max_size=24))
def test_the_trailing_mean_itself_is_never_anomalous(counts: list[int]) -> None:
    history = hourly(MONDAY_9 - timedelta(hours=len(counts)), counts)
    mean = round(sum(counts) / len(counts))

    assert volume_anomaly(history, HourlyCount(MONDAY_9, mean)).status is Status.PASS


def test_freshness_breach_and_missing_table() -> None:
    sla = FreshnessSla("flight_schedules", timedelta(minutes=30))
    now = MONDAY_9

    fresh = evaluate_freshness(sla, now - timedelta(minutes=10), now)
    stale = evaluate_freshness(sla, now - timedelta(hours=2), now)
    missing = evaluate_freshness(sla, None, now)

    assert fresh.status is Status.PASS
    assert stale.status is Status.FAIL
    assert stale.observed == pytest.approx(7200)
    assert missing.status is Status.FAIL


def test_null_rate_drift_flags_columns_that_moved_beyond_tolerance() -> None:
    results = null_rate_drift(
        "flight_schedules",
        baseline={"revised_departure_utc": 0.30, "callsign": 0.02},
        current={"revised_departure_utc": 0.35, "callsign": 0.40, "new_column": 0.9},
        tolerance=0.10,
    )

    by_column = {result.check: result.status for result in results}
    assert by_column == {
        "null_rate_drift:revised_departure_utc": Status.PASS,
        "null_rate_drift:callsign": Status.FAIL,
    }
