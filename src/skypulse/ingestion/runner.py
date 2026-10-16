"""Runs a poller on a fixed interval until stopped.

A failed cycle is logged and the loop carries on: one bad response from an upstream API
must not take a producer down. Credit exhaustion backs off for longer.
"""

import asyncio
from dataclasses import dataclass

from skypulse.common.correlation import correlation_scope
from skypulse.common.http import Sleep
from skypulse.common.logging import get_logger
from skypulse.ingestion.opensky.access import CreditsExhaustedError
from skypulse.ingestion.pollers import Poller
from skypulse.observability.metrics import (
    EVENTS_PUBLISHED,
    EVENTS_REJECTED,
    POLL_DURATION,
    POLL_FAILURES,
)

CREDITS_BACKOFF_SECONDS = 900.0

_log = get_logger(__name__)


@dataclass(frozen=True)
class Schedule:
    interval_seconds: float
    cycles: int | None = None

    def allows(self, completed: int) -> bool:
        return self.cycles is None or completed < self.cycles


async def run_cycle(name: str, poller: Poller) -> float | None:
    """Runs one cycle and returns a back-off override when the cycle could not run."""
    with correlation_scope(), POLL_DURATION.labels(name).time():
        try:
            report = await poller.run_once()
        except CreditsExhaustedError as error:
            _log.warning("poll_out_of_credits", poller=name, reason=str(error))
            POLL_FAILURES.labels(name).inc()
            return CREDITS_BACKOFF_SECONDS
        except Exception:
            _log.exception("poll_failed", poller=name)
            POLL_FAILURES.labels(name).inc()
            return None
        EVENTS_PUBLISHED.labels(name).inc(report.published)
        EVENTS_REJECTED.labels(name).inc(report.rejected)
        _log.info(
            "poll_completed",
            poller=name,
            published=report.published,
            rejected=report.rejected,
        )
        return None


async def run_periodically(
    name: str, poller: Poller, schedule: Schedule, sleep: Sleep = asyncio.sleep
) -> int:
    completed = 0
    while schedule.allows(completed):
        backoff = await run_cycle(name, poller)
        completed += 1
        if schedule.allows(completed):
            await sleep(backoff or schedule.interval_seconds)
    return completed
