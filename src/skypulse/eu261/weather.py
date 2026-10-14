"""Weather that makes an "extraordinary circumstances" exemption likely.

This is a heuristic flag for analysis. Whether an exemption actually applies is decided
case by case; the thresholds below are configurable and deliberately conservative.
"""

import math
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

_THUNDERSTORM = re.compile(r"TS")
_FREEZING_PRECIPITATION = re.compile(r"FZ(RA|DZ|UP)")


class SevereCondition(StrEnum):
    THUNDERSTORM = "thunderstorm"
    FREEZING_PRECIPITATION = "freezing_precipitation"
    LOW_VISIBILITY = "low_visibility"
    STRONG_CROSSWIND = "strong_crosswind"


@dataclass(frozen=True)
class Wind:
    direction_degrees: int | None
    speed_kt: int | None
    gust_kt: int | None

    def peak_kt(self) -> float:
        return float(max(self.speed_kt or 0, self.gust_kt or 0))


@dataclass(frozen=True)
class WeatherObservation:
    observed_at: datetime
    present_weather: str | None
    visibility_m: int | None
    wind: Wind


@dataclass(frozen=True)
class WeatherPolicy:
    minimum_visibility_m: int = 800
    maximum_crosswind_kt: float = 35.0
    window: timedelta = timedelta(hours=1)


def crosswind_kt(wind: Wind, runway_headings: tuple[int, ...]) -> float:
    """Crosswind on the best-aligned runway; the full wind when that can't be known."""
    peak = wind.peak_kt()
    if wind.direction_degrees is None or not runway_headings:
        return peak
    return min(
        abs(peak * math.sin(math.radians(wind.direction_degrees - heading)))
        for heading in runway_headings
    )


def severe_conditions(
    observation: WeatherObservation, policy: WeatherPolicy, runway_headings: tuple[int, ...] = ()
) -> frozenset[SevereCondition]:
    weather = observation.present_weather or ""
    visibility = observation.visibility_m
    checks = {
        SevereCondition.THUNDERSTORM: bool(_THUNDERSTORM.search(weather)),
        SevereCondition.FREEZING_PRECIPITATION: bool(_FREEZING_PRECIPITATION.search(weather)),
        SevereCondition.LOW_VISIBILITY: visibility is not None
        and visibility < policy.minimum_visibility_m,
        SevereCondition.STRONG_CROSSWIND: crosswind_kt(observation.wind, runway_headings)
        > policy.maximum_crosswind_kt,
    }
    return frozenset(condition for condition, present in checks.items() if present)


def severe_weather_around(
    observations: Iterable[WeatherObservation],
    moment: datetime,
    policy: WeatherPolicy,
    runway_headings: tuple[int, ...] = (),
) -> frozenset[SevereCondition]:
    """Severe conditions reported within the policy window around ``moment``."""
    nearby = (item for item in observations if abs(item.observed_at - moment) <= policy.window)
    found: set[SevereCondition] = set()
    for item in nearby:
        found |= severe_conditions(item, policy, runway_headings)
    return frozenset(found)
