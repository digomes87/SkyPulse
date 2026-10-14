"""Small aviation value types shared across sources."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from skypulse.common.clock import require_aware

_ICAO_AIRPORT = re.compile(r"^[A-Z0-9]{4}$")


@dataclass(frozen=True)
class AirportIcao:
    code: str

    def __post_init__(self) -> None:
        if not _ICAO_AIRPORT.fullmatch(self.code):
            raise ValueError(f"not an ICAO airport code: {self.code!r}")

    def __str__(self) -> str:
        return self.code


@dataclass(frozen=True)
class TimeWindow:
    """Half-open interval [start, end) in UTC."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        require_aware(self.start)
        require_aware(self.end)
        if self.end <= self.start:
            raise ValueError("time window end must be after its start")

    def duration(self) -> timedelta:
        return self.end - self.start

    def split(self, size: timedelta) -> list["TimeWindow"]:
        """Consecutive windows no longer than ``size`` covering this one."""
        starts = []
        cursor = self.start
        while cursor < self.end:
            starts.append(cursor)
            cursor += size
        return [TimeWindow(start, min(start + size, self.end)) for start in starts]
