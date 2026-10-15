"""Structured Streaming progress as Prometheus metrics.

The Kafka source reports how far each micro-batch is behind the end of the topic
(``maxOffsetsBehindLatest``), which is the consumer lag: Spark tracks offsets in its
checkpoint, not in a Kafka consumer group, so a lag exporter would see nothing.
"""

from datetime import UTC, datetime
from typing import Any

from prometheus_client import Counter, Gauge
from pyspark.sql.streaming.listener import (
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
    StreamingQueryListener,
)

from skypulse.common.config import Settings
from skypulse.observability.metrics import push_registry

LABELS = ["query"]
INPUT_ROWS = Gauge("skypulse_stream_input_rows", "Rows in the last micro-batch", LABELS)
INPUT_RATE = Gauge("skypulse_stream_input_rows_per_second", "Input rate", LABELS)
PROCESSED_RATE = Gauge("skypulse_stream_processed_rows_per_second", "Processing rate", LABELS)
BATCH_DURATION = Gauge("skypulse_stream_batch_duration_ms", "Micro-batch duration", LABELS)
OFFSETS_BEHIND = Gauge(
    "skypulse_stream_offsets_behind_latest", "Kafka offsets not yet read (consumer lag)", LABELS
)
WATERMARK_DELAY = Gauge(
    "skypulse_stream_watermark_delay_seconds", "Wall clock minus the event-time watermark", LABELS
)
BATCHES = Counter("skypulse_stream_batches", "Micro-batches completed", LABELS)
TERMINATIONS = Counter("skypulse_stream_terminations", "Queries that stopped", ["query", "failed"])


def _offsets_behind(sources: list[Any]) -> float:
    behind = [float(source.metrics.get("maxOffsetsBehindLatest", 0) or 0) for source in sources]
    return max(behind, default=0.0)


def _watermark_delay(event_time: dict[str, str]) -> float | None:
    watermark = event_time.get("watermark")
    if not watermark:
        return None
    moment = datetime.fromisoformat(watermark.replace("Z", "+00:00"))
    if moment.year < 2000:  # epoch: no watermark yet
        return None
    return (datetime.now(UTC) - moment).total_seconds()


def record_progress(progress: Any) -> None:
    query = progress.name or str(progress.id)
    INPUT_ROWS.labels(query).set(progress.numInputRows)
    INPUT_RATE.labels(query).set(progress.inputRowsPerSecond or 0)
    PROCESSED_RATE.labels(query).set(progress.processedRowsPerSecond or 0)
    BATCH_DURATION.labels(query).set(progress.batchDuration)
    OFFSETS_BEHIND.labels(query).set(_offsets_behind(list(progress.sources)))
    delay = _watermark_delay(dict(progress.eventTime))
    if delay is not None:
        WATERMARK_DELAY.labels(query).set(delay)
    BATCHES.labels(query).inc()


class MetricsListener(StreamingQueryListener):
    def __init__(self) -> None:
        self._names: dict[str, str] = {}

    def onQueryStarted(self, event: QueryStartedEvent) -> None:  # noqa: N802 - Spark API
        self._names[str(event.id)] = event.name or str(event.id)

    def onQueryProgress(self, event: QueryProgressEvent) -> None:  # noqa: N802 - Spark API
        record_progress(event.progress)

    def onQueryIdle(self, event: QueryIdleEvent) -> None:  # noqa: N802, ARG002 - Spark API
        return None

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:  # noqa: N802 - Spark API
        name = self._names.get(str(event.id), str(event.id))
        TERMINATIONS.labels(name, str(event.exception is not None).lower()).inc()


def push_metrics(settings: Settings, job: str) -> bool:
    """Batch-style runs end before Prometheus scrapes them, so they push instead."""
    return push_registry(settings, job)
