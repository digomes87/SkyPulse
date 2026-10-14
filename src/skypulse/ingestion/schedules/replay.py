"""Replays recorded FIDS snapshots so the pipeline runs offline and deterministically.

Layout: ``<root>/<ICAO>/<YYYYMMDDTHHMMZ>.json``, one raw FIDS payload per file, named
after the moment it was taken. The provider serves the newest snapshot that is not later
than the clock, so advancing a replay clock makes flights progress through their statuses.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

from skypulse.common.aviation import TimeWindow
from skypulse.common.clock import Clock
from skypulse.ingestion.airports import MonitoredAirport
from skypulse.ingestion.schedules.models import ScheduleBatch

SNAPSHOT_FORMAT = "%Y%m%dT%H%MZ"


def snapshot_time(path: Path) -> datetime:
    return datetime.strptime(path.stem, SNAPSHOT_FORMAT).replace(tzinfo=UTC)


class ReplayProvider:
    def __init__(self, root: Path, clock: Clock) -> None:
        self._root = root
        self._clock = clock

    async def flights(self, airport: MonitoredAirport, window: TimeWindow) -> ScheduleBatch:
        snapshot = self._latest_snapshot(airport)
        if snapshot is None:
            return ScheduleBatch(())
        payload = json.loads(snapshot.read_text())
        batch = ScheduleBatch.from_fids(payload, airport.icao, snapshot_time(snapshot))
        return batch.within(window, airport.icao)

    def _latest_snapshot(self, airport: MonitoredAirport) -> Path | None:
        now = self._clock.now()
        candidates = sorted((self._root / str(airport.icao)).glob("*.json"))
        eligible = [path for path in candidates if snapshot_time(path) <= now]
        return eligible[-1] if eligible else None
