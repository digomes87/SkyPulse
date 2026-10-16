"""Grafana dashboards and alert rules, generated from code.

``python -m skypulse.observability.dashboards`` writes the JSON that Grafana provisions
from ``observability/grafana``. A unit test regenerates it and fails on drift, and checks
every query only uses metrics SkyPulse actually exports.
"""

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DATASOURCE = {"type": "prometheus", "uid": "prometheus"}
OUTPUT = Path(__file__).resolve().parents[3] / "observability" / "grafana"
GRID_COLUMNS = 24
SERIES_BLUE = "#2a78d6"
SERIES_ORANGE = "#eb6834"
CRITICAL = "#d03b3b"
GOOD = "#0ca30c"


@dataclass(frozen=True)
class Query:
    expr: str
    legend: str = ""


@dataclass(frozen=True)
class Panel:
    title: str
    queries: tuple[Query, ...]
    kind: str = "timeseries"
    unit: str = "short"
    width: int = 12
    height: int = 8
    description: str = ""
    red_above: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def render(self, panel_id: int, x: int, y: int) -> dict[str, Any]:
        defaults: dict[str, Any] = {
            "unit": self.unit,
            "color": {"mode": "fixed", "fixedColor": SERIES_BLUE},
        }
        if self.red_above is not None:
            defaults["color"] = {"mode": "thresholds"}
            defaults["thresholds"] = {
                "mode": "absolute",
                "steps": [
                    {"color": GOOD, "value": None},
                    {"color": CRITICAL, "value": self.red_above},
                ],
            }
        return {
            "id": panel_id,
            "type": self.kind,
            "title": self.title,
            "description": self.description,
            "datasource": DATASOURCE,
            "gridPos": {"x": x, "y": y, "w": self.width, "h": self.height},
            "targets": [
                {
                    "refId": chr(65 + index),
                    "expr": query.expr,
                    "legendFormat": query.legend,
                    "datasource": DATASOURCE,
                }
                for index, query in enumerate(self.queries)
            ],
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}, **self.extra},
        }


def _layout(panels: Sequence[Panel]) -> list[dict[str, Any]]:
    rendered = []
    x = y = row_height = 0
    for panel_id, panel in enumerate(panels, start=1):
        if x + panel.width > GRID_COLUMNS:
            x, y, row_height = 0, y + row_height, 0
        rendered.append(panel.render(panel_id, x, y))
        x += panel.width
        row_height = max(row_height, panel.height)
    return rendered


def dashboard(uid: str, title: str, description: str, panels: Sequence[Panel]) -> dict[str, Any]:
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "tags": ["skypulse"],
        "schemaVersion": 39,
        "editable": False,
        "time": {"from": "now-6h", "to": "now"},
        "refresh": "30s",
        "panels": _layout(panels),
    }


# Counts of bad things read 0 rather than "No data" before the first one happens.
ZERO_IF_ABSENT = " or vector(0)"


def _stat(
    title: str,
    expr: str,
    unit: str = "short",
    red_above: float | None = None,
    description: str = "",
) -> Panel:
    query = Query(expr + ZERO_IF_ABSENT if red_above is not None else expr)
    return Panel(
        title,
        (query,),
        "stat",
        unit,
        width=6,
        height=4,
        description=description,
        red_above=red_above,
    )


PIPELINE_HEALTH = dashboard(
    "skypulse-pipeline",
    "SkyPulse · Pipeline health",
    "Producers, streaming jobs and Kafka lag.",
    [
        _stat("Events published (1h)", "sum(increase(skypulse_events_published_total[1h]))"),
        _stat(
            "Dead-lettered events (1h)",
            "sum(increase(skypulse_events_rejected_total[1h]))",
            red_above=1,
        ),
        _stat("Failed polls (1h)", "sum(increase(skypulse_poll_failures_total[1h]))", red_above=1),
        _stat("OpenSky credits left", "max(skypulse_opensky_credits_remaining)"),
        Panel(
            "Events published per second",
            (Query("sum by (feed) (rate(skypulse_events_published_total[5m]))", "{{feed}}"),),
            unit="ops",
        ),
        Panel(
            "Poll duration p95",
            (
                Query(
                    "histogram_quantile(0.95, sum by (le, feed) "
                    "(rate(skypulse_poll_duration_seconds_bucket[5m])))",
                    "{{feed}}",
                ),
            ),
            unit="s",
        ),
        Panel(
            "Kafka lag (offsets behind latest)",
            (Query("max by (query) (skypulse_stream_offsets_behind_latest)", "{{query}}"),),
            description=(
                "Spark tracks offsets in its checkpoint; the Kafka source reports this "
                "on every micro-batch."
            ),
        ),
        Panel(
            "Rows processed per second",
            (Query("sum by (query) (skypulse_stream_processed_rows_per_second)", "{{query}}"),),
            unit="ops",
        ),
        Panel(
            "Micro-batch duration",
            (Query("max by (query) (skypulse_stream_batch_duration_ms)", "{{query}}"),),
            unit="ms",
        ),
        Panel(
            "Watermark delay",
            (Query("max by (query) (skypulse_stream_watermark_delay_seconds)", "{{query}}"),),
            unit="s",
        ),
        Panel(
            "HTTP retries per minute",
            (Query("sum by (host) (rate(skypulse_http_retries_total[5m])) * 60", "{{host}}"),),
            width=24,
        ),
    ],
)

DATA_QUALITY = dashboard(
    "skypulse-quality",
    "SkyPulse · Data quality and freshness",
    "Table freshness against SLAs, quality gates, dead letters and claims.",
    [
        _stat(
            "Tables breaking freshness SLA",
            "sum(skypulse_table_freshness_sla_breached)",
            red_above=1,
        ),
        _stat("Failed quality checks", "sum(skypulse_quality_checks_failed)", red_above=1),
        _stat("Messages in SQS DLQs", "sum(skypulse_dlq_messages)", red_above=1),
        _stat("EU261 claims stored", "max(skypulse_eu261_claims)"),
        Panel(
            "Minutes since last change, per table",
            (
                Query(
                    "(time() - skypulse_table_last_commit_timestamp_seconds) / 60",
                    "{{layer}}.{{table}}",
                ),
            ),
            "bargauge",
            unit="m",
            width=12,
            height=10,
            extra={"orientation": "horizontal", "displayMode": "basic"},
        ),
        Panel(
            "Failing quality checks",
            (Query("skypulse_quality_check_status == 0", "{{suite}} · {{check}}"),),
            "table",
            width=12,
            height=10,
            description="Checks whose last run failed (1 pass, 0 fail, -1 skipped).",
        ),
        Panel(
            "Dead letters",
            (
                Query("skypulse_dlq_messages", "SQS {{queue}}"),
                Query("skypulse_kafka_topic_messages", "Kafka {{topic}}"),
            ),
            width=24,
            extra={"tooltip": {"mode": "multi"}},
        ),
    ],
)


@dataclass(frozen=True)
class AlertRule:
    """Fires when ``expr`` stays above ``above`` for ``wait``; the uid names its runbook."""

    uid: str
    title: str
    expr: str
    above: float
    wait: str
    summary: str

    def render(self) -> dict[str, Any]:
        query = {"refId": "A", "expr": self.expr, "instant": True}
        threshold = {
            "refId": "C",
            "type": "threshold",
            "expression": "A",
            "conditions": [{"evaluator": {"type": "gt", "params": [self.above]}}],
        }
        return {
            "uid": self.uid,
            "title": self.title,
            "condition": "C",
            "for": self.wait,
            "labels": {"team": "data-platform"},
            "annotations": {"summary": self.summary, "runbook_url": f"docs/runbooks/{self.uid}.md"},
            "noDataState": "OK",
            "execErrState": "Error",
            "data": [
                {
                    "refId": "A",
                    "relativeTimeRange": {"from": 600, "to": 0},
                    "datasourceUid": DATASOURCE["uid"],
                    "model": query,
                },
                {"refId": "C", "datasourceUid": "__expr__", "model": threshold},
            ],
        }


RULES = (
    AlertRule(
        "freshness-sla-breach",
        "Table freshness SLA breached",
        "sum(skypulse_table_freshness_sla_breached)",
        0,
        "10m",
        "A lake table has not changed within its freshness SLA.",
    ),
    AlertRule(
        "quality-gate-failure",
        "Quality checks failing",
        "sum(skypulse_quality_checks_failed)",
        0,
        "0m",
        "A data quality suite failed; downstream tasks were stopped.",
    ),
    AlertRule(
        "sqs-dead-letter-redrive",
        "Messages in a dead-letter queue",
        "sum(skypulse_dlq_messages)",
        0,
        "5m",
        "Disruption messages failed three times and wait in a DLQ.",
    ),
    AlertRule(
        "kafka-lag",
        "Streaming is falling behind Kafka",
        "max(skypulse_stream_offsets_behind_latest)",
        10000,
        "15m",
        "A streaming query is more than 10k offsets behind.",
    ),
)

ALERT_RULES = {
    "apiVersion": 1,
    "groups": [
        {
            "orgId": 1,
            "name": "skypulse",
            "folder": "SkyPulse",
            "interval": "1m",
            "rules": [rule.render() for rule in RULES],
        }
    ],
}

OUTPUTS = {
    "dashboards/pipeline-health.json": PIPELINE_HEALTH,
    "dashboards/data-quality.json": DATA_QUALITY,
    "provisioning/alerting/rules.json": ALERT_RULES,
}


def render(document: dict[str, Any]) -> str:
    return json.dumps(document, indent=2, ensure_ascii=False) + "\n"


def main() -> None:
    for relative, document in OUTPUTS.items():
        path = OUTPUT / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render(document))


if __name__ == "__main__":
    main()
