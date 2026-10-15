"""The outcome of one quality check, shared by expectation suites and statistical checks."""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from enum import StrEnum


class Status(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"


@dataclass(frozen=True)
class CheckResult:
    suite: str
    check: str
    status: Status
    observed: float | None = None
    threshold: float | None = None
    details: str = ""


@dataclass(frozen=True)
class QualityReport:
    results: tuple[CheckResult, ...] = field(default_factory=tuple)

    def merged_with(self, others: Sequence[CheckResult]) -> "QualityReport":
        return QualityReport((*self.results, *others))

    def failures(self) -> list[CheckResult]:
        return [result for result in self.results if result.status is Status.FAIL]

    def passed(self) -> bool:
        return not self.failures()

    def __iter__(self) -> Iterator[CheckResult]:
        return iter(self.results)

    def __len__(self) -> int:
        return len(self.results)


class QualityGateError(RuntimeError):
    """Raised when a suite fails, so the orchestrator stops downstream tasks."""

    def __init__(self, report: QualityReport) -> None:
        names = ", ".join(f"{failure.suite}.{failure.check}" for failure in report.failures())
        super().__init__(f"{len(report.failures())} quality checks failed: {names}")
        self.report = report
