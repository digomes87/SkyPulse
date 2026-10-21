"""Quality screen for reference data, run before a load replaces the silver tables.

A truncated or empty download must never overwrite good reference data: the detector and
the EU261 rules depend on it. A table fails the screen when it has no rows, or when it
shrank below ``min_ratio`` of the rows it holds now.
"""

from collections.abc import Mapping


class ReferenceScreenError(RuntimeError):
    """Raised before any table is written, so a failed load leaves silver untouched."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("reference data rejected: " + "; ".join(problems))
        self.problems = problems


def _problem(table: str, rows: int, before: int | None, min_ratio: float) -> str | None:
    if rows == 0:
        return f"{table} has no rows"
    if before is None or rows >= before * min_ratio:
        return None
    return f"{table} has {rows} rows, {before} before (minimum {min_ratio:.0%})"


def screen(current: Mapping[str, int], previous: Mapping[str, int], min_ratio: float) -> list[str]:
    """Problems with ``current`` row counts against ``previous`` ones; empty when it passes."""
    found = (
        _problem(table, rows, previous.get(table), min_ratio) for table, rows in current.items()
    )
    return [problem for problem in found if problem is not None]
