"""Statistical checks that complement the rule-based expectations.

- Volume anomaly: hourly counts compared with a seasonal baseline (same hour of the week),
  falling back to a trailing window. Counts are noisy even when nothing is wrong, so the
  spread has a Poisson floor (sqrt of the mean): a flat history never makes every small
  wobble an anomaly.
- Freshness: time since a table last changed against a per-table SLA.
- Null-rate drift: share of nulls per column against a baseline.
"""

import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from skypulse.quality.results import CheckResult, Status

Z_THRESHOLD = 3.0
MIN_SEASONAL_POINTS = 3
MIN_TRAILING_POINTS = 6
TRAILING_WINDOW = 24
DEFAULT_NULL_TOLERANCE = 0.10


@dataclass(frozen=True)
class HourlyCount:
    hour: datetime
    count: int

    def hour_of_week(self) -> tuple[int, int]:
        return self.hour.weekday(), self.hour.hour


@dataclass(frozen=True)
class VolumeCheck:
    result: CheckResult
    method: str
    expected: float

    @property
    def status(self) -> Status:
        return self.result.status


def _baseline(history: Sequence[HourlyCount], current: HourlyCount) -> tuple[str, list[int]]:
    seasonal = [item.count for item in history if item.hour_of_week() == current.hour_of_week()]
    if len(seasonal) >= MIN_SEASONAL_POINTS:
        return "seasonal", seasonal
    trailing = [item.count for item in sorted(history, key=lambda item: item.hour)][
        -TRAILING_WINDOW:
    ]
    if len(trailing) >= MIN_TRAILING_POINTS:
        return "trailing", trailing
    return "none", trailing


def volume_anomaly(
    history: Sequence[HourlyCount], current: HourlyCount, threshold: float = Z_THRESHOLD
) -> VolumeCheck:
    method, baseline = _baseline(history, current)
    check = f"volume:{current.hour:%Y-%m-%dT%H}"
    if method == "none":
        result = CheckResult("statistics", check, Status.SKIP, current.count, threshold,
                             f"only {len(baseline)} hours of history")  # fmt: skip
        return VolumeCheck(result, method, math.nan)
    expected = statistics.fmean(baseline)
    spread = max(statistics.pstdev(baseline), math.sqrt(max(expected, 1.0)))
    z_score = (current.count - expected) / spread
    status = Status.FAIL if abs(z_score) > threshold else Status.PASS
    details = f"{method} baseline {expected:.1f} ± {spread:.1f}, observed {current.count}"
    return VolumeCheck(
        CheckResult("statistics", check, status, z_score, threshold, details), method, expected
    )


@dataclass(frozen=True)
class FreshnessSla:
    table: str
    max_age: timedelta


FRESHNESS_SLAS = (
    FreshnessSla("aircraft_states", timedelta(minutes=15)),
    FreshnessSla("flight_schedules", timedelta(minutes=30)),
    FreshnessSla("metar_observations", timedelta(minutes=90)),
    FreshnessSla("flight_disruptions", timedelta(hours=2)),
    FreshnessSla("eu261_assessments", timedelta(hours=2)),
    FreshnessSla("reference_airports", timedelta(days=8)),
)


def evaluate_freshness(
    sla: FreshnessSla, last_updated: datetime | None, now: datetime
) -> CheckResult:
    limit = sla.max_age.total_seconds()
    check = f"freshness:{sla.table}"
    if last_updated is None:
        return CheckResult(
            "freshness", check, Status.FAIL, None, limit, "table has never been written"
        )
    age = (now - last_updated).total_seconds()
    status = Status.FAIL if age > limit else Status.PASS
    return CheckResult("freshness", check, status, age, limit, f"last change {age:.0f}s ago")


def null_rate_drift(
    table: str,
    baseline: Mapping[str, float],
    current: Mapping[str, float],
    tolerance: float = DEFAULT_NULL_TOLERANCE,
) -> list[CheckResult]:
    """Compares columns present in both; new or dropped columns are a schema matter, not drift."""
    shared = sorted(baseline.keys() & current.keys())
    return [
        CheckResult(
            f"null_drift:{table}",
            f"null_rate_drift:{column}",
            Status.FAIL if abs(current[column] - baseline[column]) > tolerance else Status.PASS,
            current[column],
            tolerance,
            f"baseline {baseline[column]:.2f}, now {current[column]:.2f}",
        )
        for column in shared
    ]
