"""Recorded snapshots on disk, named after the UTC minute they were taken.

``<YYYYMMDDTHHMMZ>.json`` or ``.json.gz``. Replay picks the newest snapshot that is not
later than the replay clock.
"""

import gzip
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SNAPSHOT_FORMAT = "%Y%m%dT%H%MZ"


@dataclass(frozen=True)
class Snapshot:
    path: Path

    def taken_at(self) -> datetime:
        stem = self.path.name.split(".", 1)[0]
        return datetime.strptime(stem, SNAPSHOT_FORMAT).replace(tzinfo=UTC)

    def load(self) -> Any:
        raw = self.path.read_bytes()
        if self.path.suffix == ".gz":
            raw = gzip.decompress(raw)
        return json.loads(raw)


@dataclass(frozen=True)
class SnapshotFolder:
    root: Path

    def latest_at(self, instant: datetime) -> Snapshot | None:
        snapshots = sorted(
            (Snapshot(path) for path in self.root.glob("*.json*")), key=Snapshot.taken_at
        )
        eligible = [snapshot for snapshot in snapshots if snapshot.taken_at() <= instant]
        return eligible[-1] if eligible else None
