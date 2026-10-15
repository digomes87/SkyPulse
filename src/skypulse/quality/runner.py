"""Runs expectation suites against Spark or pandas data frames and reports CheckResults."""

from collections.abc import Callable
from typing import Any

import great_expectations as gx
from great_expectations.core import ExpectationValidationResult
from great_expectations.data_context import AbstractDataContext
from great_expectations.expectations import Expectation

from skypulse.quality.results import CheckResult, QualityReport, Status


class SuiteRunner:
    """One ephemeral GX context; data sources for Spark and pandas created on demand."""

    def __init__(self) -> None:
        self._context: AbstractDataContext = gx.get_context(mode="ephemeral")

    def validate_spark(
        self, name: str, frame: Any, expectations: list[Expectation]
    ) -> QualityReport:
        return self._validate(
            name, frame, expectations, self._context.data_sources.add_or_update_spark
        )

    def validate_pandas(
        self, name: str, frame: Any, expectations: list[Expectation]
    ) -> QualityReport:
        return self._validate(
            name, frame, expectations, self._context.data_sources.add_or_update_pandas
        )

    def _validate(
        self,
        name: str,
        frame: Any,
        expectations: list[Expectation],
        add_source: Callable[[str], Any],
    ) -> QualityReport:
        source = add_source(f"source_{name}")
        batch_definition = source.add_dataframe_asset(name).add_batch_definition_whole_dataframe(
            "all"
        )
        suite = self._context.suites.add_or_update(gx.ExpectationSuite(name=name))
        for expectation in expectations:
            suite.add_expectation(expectation)
        batch = batch_definition.get_batch(batch_parameters={"dataframe": frame})
        outcome = batch.validate(suite)
        return QualityReport(tuple(_check(name, result) for result in outcome.results))


def _check(suite: str, result: ExpectationValidationResult) -> CheckResult:
    config = result.expectation_config
    kwargs = config.kwargs if config is not None else {}
    target = kwargs.get("column") or ",".join(kwargs.get("column_list", [])) or "table"
    details = result.result or {}
    observed = details.get("unexpected_percent", details.get("observed_value"))
    return CheckResult(
        suite=suite,
        check=f"{config.type if config is not None else 'unknown'}:{target}",
        status=Status.PASS if result.success else Status.FAIL,
        observed=float(observed) if isinstance(observed, int | float) else None,
        details=str(
            {key: value for key, value in details.items() if key != "partial_unexpected_list"}
        )[:500],
    )
