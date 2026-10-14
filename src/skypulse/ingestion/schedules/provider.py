"""Where scheduled and actual flight times come from.

OpenSky has no schedules, so SkyPulse needs a second provider. Production uses AeroDataBox;
demo mode and CI use recorded snapshots, parsed by the same code path.
"""

from typing import Protocol

from skypulse.common.aviation import TimeWindow
from skypulse.ingestion.airports import MonitoredAirport
from skypulse.ingestion.schedules.models import ScheduleBatch


class ScheduleProvider(Protocol):
    async def flights(self, airport: MonitoredAirport, window: TimeWindow) -> ScheduleBatch: ...
