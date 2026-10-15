"""Prometheus metrics for SkyPulse processes.

Long-running processes (producers, continuous streaming) expose them on
``SKYPULSE_METRICS__PORT``; batch runs push to ``SKYPULSE_METRICS__PUSHGATEWAY``.
"""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from skypulse.common.config import Settings

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


def start_metrics_server(settings: Settings) -> bool:
    if settings.metrics.port is None:
        return False
    start_http_server(settings.metrics.port)
    return True
