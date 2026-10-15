import json
import re

from prometheus_client import REGISTRY

import skypulse.observability.exporter
import skypulse.observability.metrics
import skypulse.observability.streaming  # noqa: F401 - registers the streaming metrics
from skypulse.observability.dashboards import OUTPUT, OUTPUTS, RULES, render
from skypulse.quality.store import QUALITY_METRICS

METRIC = re.compile(r"\bskypulse_[a-z0-9_]+")
SUFFIXES = ("_bucket", "_count", "_sum", "_total", "_created")


def base_name(sample: str) -> str:
    for suffix in SUFFIXES:
        if sample.endswith(suffix):
            return sample[: -len(suffix)]
    return sample


def exported_metrics() -> set[str]:
    names = {family.name for family in REGISTRY.collect()}
    return {name for name in names if name.startswith("skypulse_")} | set(QUALITY_METRICS)


def test_committed_files_match_the_generator() -> None:
    for relative, document in OUTPUTS.items():
        committed = (OUTPUT / relative).read_text()
        stale = f"{relative} is stale: run python -m skypulse.observability.dashboards"
        assert committed == render(document), stale


def test_every_query_uses_a_metric_skypulse_exports() -> None:
    queries = " ".join(json.dumps(document) for document in OUTPUTS.values())
    used = {base_name(name) for name in METRIC.findall(queries)}

    assert used <= exported_metrics(), used - exported_metrics()


def test_every_alert_links_an_existing_runbook() -> None:
    for rule in RULES:
        assert (OUTPUT.parents[1] / "docs" / "runbooks" / f"{rule.uid}.md").exists(), rule.uid


def test_dashboards_have_unique_panel_ids_and_fit_the_grid() -> None:
    for relative, document in OUTPUTS.items():
        if not relative.startswith("dashboards/"):
            continue
        panels = document["panels"]
        assert len({panel["id"] for panel in panels}) == len(panels)
        assert all(panel["gridPos"]["x"] + panel["gridPos"]["w"] <= 24 for panel in panels)
