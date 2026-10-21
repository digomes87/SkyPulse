"""Prometheus metrics for SkyPulse processes.

Long-running processes (producers, continuous streaming) expose them on
``SKYPULSE_METRICS__PORT``; batch runs push to ``SKYPULSE_METRICS__PUSHGATEWAY``.
"""

from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
    start_http_server,
)

from skypulse.common.config import Settings
from skypulse.common.logging import get_logger

_log = get_logger(__name__)

EVENTS_PUBLISHED = Counter("skypulse_events_published", "Events published to the stream", ["feed"])
EVENTS_REJECTED = Counter(
    "skypulse_events_rejected", "Events sent to the dead-letter topic", ["feed"]
)
POLL_FAILURES = Counter("skypulse_poll_failures", "Polling cycles that raised", ["feed"])
POLL_DURATION = Histogram(
    "skypulse_poll_duration_seconds",
    "Duration of one polling cycle",
    ["feed"],
    buckets=(0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
OPENSKY_CREDITS = Gauge(
    "skypulse_opensky_credits_remaining", "OpenSky credits left today, as last reported"
)
HTTP_RETRIES = Counter("skypulse_http_retries", "HTTP requests retried", ["host"])
LATE_ROWS_RECOVERED = Gauge(
    "skypulse_backfill_recovered_rows",
    "Rows the last backfill run inserted into silver from bronze",
    ["table"],
)


def start_metrics_server(settings: Settings, default_port: int | None = None) -> bool:
    port = settings.metrics.port or default_port
    if port is None:
        return False
    start_http_server(port)
    return True


def push_registry(settings: Settings, job: str, registry: CollectorRegistry = REGISTRY) -> bool:
    """Best effort: a missing Pushgateway is logged, never allowed to fail the run."""
    if settings.metrics.pushgateway is None:
        return False
    try:
        push_to_gateway(settings.metrics.pushgateway, job=job, registry=registry)
    except OSError as error:
        _log.warning(
            "metrics_push_failed", gateway=settings.metrics.pushgateway, job=job, error=str(error)
        )
        return False
    return True
