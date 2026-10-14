"""Replays recorded FIDS snapshots so the pipeline runs offline and deterministically.

Layout: ``<root>/<ICAO>/<YYYYMMDDTHHMMZ>.json``, one raw FIDS payload per file, named
after the moment it was taken. The provider serves the newest snapshot that is not later
than the clock, so advancing a replay clock makes flights progress through their statuses.
"""

from pathlib import Path

from skypulse.common.aviation import TimeWindow
from skypulse.common.clock import Clock
from skypulse.ingestion.airports import MonitoredAirport
from skypulse.ingestion.schedules.models import ScheduleBatch
from skypulse.ingestion.snapshots import SnapshotFolder


class ReplayProvider:
    def __init__(self, root: Path, clock: Clock) -> None:
        self._root = root
        self._clock = clock

    async def flights(self, airport: MonitoredAirport, window: TimeWindow) -> ScheduleBatch:
        snapshot = SnapshotFolder(self._root / str(airport.icao)).latest_at(self._clock.now())
        if snapshot is None:
            return ScheduleBatch(())
        batch = ScheduleBatch.from_fids(snapshot.load(), airport.icao, snapshot.taken_at())
        return batch.within(window, airport.icao)
