"""Time helpers. Components receive a Clock so tests can freeze time."""

from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class FrozenClock:
    def __init__(self, instant: datetime) -> None:
        self._instant = require_aware(instant)

    def now(self) -> datetime:
        return self._instant


def require_aware(instant: datetime) -> datetime:
    if instant.tzinfo is None:
        raise ValueError("datetime must be timezone aware")
    return instant


def utc_iso(instant: datetime) -> str:
    """RFC 3339 in UTC with a Z suffix and second precision, e.g. 2026-10-14T09:30:00Z."""
    return require_aware(instant).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def from_unix(seconds: int) -> datetime:
    return datetime.fromtimestamp(seconds, UTC)
