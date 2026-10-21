"""Reconciliation: nothing that entered bronze goes missing on its way to gold.

- bronze -> silver: every valid event's natural key is in its silver table.
- bronze -> quarantine: every invalid record (topic, partition, offset) is in quarantine.
- silver -> gold: the facts keep one row per silver row (``fct_flight`` per schedule,
  ``fct_eu261_claim`` per assessment).

Bronze is checked over the rows ingested in the last ``RECONCILIATION_WINDOW``, read with
the same parsing as the silver jobs, so a check never disagrees with the pipeline about
what "valid" means. Every check is reported, matching or not.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import Column, DataFrame, functions

from skypulse.quality.results import CheckResult, Status
from skypulse.streaming.silver import SilverTable, parse, quarantined_rows, valid_rows

RECONCILIATION_WINDOW = timedelta(hours=24)
RECORD_COORDINATES = ("topic", "partition", "offset")


@dataclass(frozen=True)
class GoldFact:
    """A gold fact that must hold exactly one row per row of a silver table."""

    silver_table: str
    fact: str


GOLD_FACTS = (
    GoldFact("flight_schedules", "fct_flight"),
    GoldFact("eu261_assessments", "fct_eu261_claim"),
)


def recent(bronze: DataFrame, now: datetime) -> DataFrame:
    start = now - RECONCILIATION_WINDOW
    return bronze.where(functions.col("ingest_date") >= functions.lit(start.date())).where(
        functions.col("ingested_at") >= functions.lit(start)
    )


def _missing(expected: DataFrame, actual: DataFrame | None, columns: tuple[str, ...]) -> int:
    """Distinct ``columns`` values of ``expected`` absent from ``actual`` (null-safe)."""
    wanted = expected.select(*columns).distinct()
    if actual is None:
        return wanted.count()
    present = actual.select(*columns).distinct()
    matches: list[Column] = [wanted[column].eqNullSafe(present[column]) for column in columns]
    return wanted.join(present, matches, "left_anti").count()


def _result(suite: str, check: str, missing: int, checked: int) -> CheckResult:
    status = Status.PASS if missing == 0 else Status.FAIL
    return CheckResult(suite, check, status, missing, 0, f"{missing} of {checked} missing")


def bronze_to_silver(
    bronze: DataFrame, silver: DataFrame | None, table: SilverTable
) -> CheckResult:
    keys = table.policy.keys
    events = valid_rows(parse(bronze, table), table)
    checked = events.select(*keys).distinct().count()
    missing = _missing(events, silver, keys)
    return _result("reconciliation.bronze_to_silver", table.name, missing, checked)


def bronze_to_quarantine(
    bronze: DataFrame, quarantine: DataFrame | None, table: SilverTable
) -> CheckResult:
    rejected = quarantined_rows(parse(bronze, table), table)
    checked = rejected.count()
    missing = _missing(rejected, quarantine, RECORD_COORDINATES)
    return _result("reconciliation.bronze_to_quarantine", table.name, missing, checked)


def silver_to_gold(fact: GoldFact, silver_rows: int, gold_rows: int) -> CheckResult:
    difference = abs(silver_rows - gold_rows)
    status = Status.PASS if difference == 0 else Status.FAIL
    details = f"silver.{fact.silver_table}={silver_rows}, gold.{fact.fact}={gold_rows}"
    return CheckResult("reconciliation.silver_to_gold", fact.fact, status, difference, 0, details)
