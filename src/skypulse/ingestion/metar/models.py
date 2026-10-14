"""METAR observations from the aviationweather.gov Data API (JSON format)."""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from skypulse.common.clock import from_unix, utc_iso
from skypulse.contracts import Event, EventSource, EventType

METRES_PER_STATUTE_MILE = 1609.344
# METAR reports 10 km or more as 9999; the API reports it as "6+" (or "10+") statute miles.
UNLIMITED_VISIBILITY_M = 9999
VARIABLE_WIND = "VRB"


def _visibility_m(raw: object) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, str) and raw.endswith("+"):
        return UNLIMITED_VISIBILITY_M
    return round(float(str(raw)) * METRES_PER_STATUTE_MILE)


def _wind_direction(raw: object) -> int | None:
    if isinstance(raw, int):
        return raw
    return None


@dataclass(frozen=True)
class MetarObservation:
    data: Mapping[str, Any]

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "MetarObservation":
        return cls(
            {
                "station_icao": payload["icaoId"],
                "observed_at": utc_iso(from_unix(payload["obsTime"])),
                "report_type": payload.get("metarType", "METAR"),
                "wind_direction_degrees": _wind_direction(payload.get("wdir")),
                "wind_variable": payload.get("wdir") == VARIABLE_WIND,
                "wind_speed_kt": payload.get("wspd"),
                "wind_gust_kt": payload.get("wgst"),
                "visibility_m": _visibility_m(payload.get("visib")),
                "present_weather": payload.get("wxString"),
                "temperature_c": payload.get("temp"),
                "dewpoint_c": payload.get("dewp"),
                "altimeter_hpa": payload.get("altim"),
                "flight_category": payload.get("fltCat"),
                "raw_text": payload["rawOb"],
            }
        )

    def to_event(self, source: EventSource, produced_at: datetime) -> Event:
        key = f"{self.data['station_icao']}|{self.data['observed_at']}|{self.data['report_type']}"
        return Event.create(EventType.METAR_OBSERVATION, source, key, self.data, produced_at)


@dataclass(frozen=True)
class MetarBatch:
    observations: tuple[MetarObservation, ...]

    @classmethod
    def from_payload(cls, payload: list[Mapping[str, Any]]) -> "MetarBatch":
        return cls(tuple(MetarObservation.from_payload(item) for item in payload))

    def merged_with(self, other: "MetarBatch") -> "MetarBatch":
        return MetarBatch(self.observations + other.observations)

    def __len__(self) -> int:
        return len(self.observations)

    def __iter__(self) -> Iterator[MetarObservation]:
        return iter(self.observations)
