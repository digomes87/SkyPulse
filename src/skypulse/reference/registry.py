"""Lookup of airports and airlines by ICAO code."""

import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from functools import cache
from importlib import resources
from typing import Any


def _load(file_name: str) -> list[dict[str, Any]]:
    raw = resources.files("skypulse.reference").joinpath(file_name).read_text()
    entries: list[dict[str, Any]] = json.loads(raw)
    return entries


@dataclass(frozen=True)
class ReferenceTable:
    """Rows of one reference file, indexed by ICAO code."""

    rows: Mapping[str, Mapping[str, Any]]

    @classmethod
    def from_file(cls, file_name: str) -> "ReferenceTable":
        return cls({row["icao"]: row for row in _load(file_name)})

    def find(self, icao: str | None) -> Mapping[str, Any] | None:
        if icao is None:
            return None
        return self.rows.get(icao)

    def __iter__(self) -> Iterator[Mapping[str, Any]]:
        return iter(self.rows.values())

    def __len__(self) -> int:
        return len(self.rows)


@cache
def airports() -> ReferenceTable:
    return ReferenceTable.from_file("airports.json")


@cache
def airlines() -> ReferenceTable:
    return ReferenceTable.from_file("airlines.json")
