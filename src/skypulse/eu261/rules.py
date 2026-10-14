"""Eligibility and compensation under Regulation (EC) 261/2004, simplified.

Simplifications, on purpose:
- Delay is the arrival delay at the final destination of this leg (Sturgeon: >= 3 hours).
- A cancellation is compensable when notified less than 14 days before departure; the
  offered-alternative exceptions of Art. 5(1)(c) are approximated by the rerouting rule.
- Rerouting within the Art. 7(2) thresholds halves the amount.
"""

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum, StrEnum

from skypulse.eu261.geography import EU261_COUNTRIES, Coordinates, CountryCode, great_circle_km
from skypulse.eu261.weather import SevereCondition

DELAY_THRESHOLD = timedelta(hours=3)
CANCELLATION_NOTICE = timedelta(days=14)
SHORT_HAUL_LIMIT_KM = 1500.0
MEDIUM_HAUL_LIMIT_KM = 3500.0


class DistanceBand(Enum):
    """Compensation band: amount in EUR and the rerouting delay that halves it."""

    SHORT = (250, timedelta(hours=2))
    MEDIUM = (400, timedelta(hours=3))
    LONG = (600, timedelta(hours=4))

    @property
    def amount_eur(self) -> int:
        return self.value[0]

    @property
    def rerouting_threshold(self) -> timedelta:
        return self.value[1]


class Verdict(StrEnum):
    NOT_COVERED = "not_covered"
    PENDING = "pending"
    DELAY_UNDER_THRESHOLD = "delay_under_threshold"
    CANCELLATION_WITH_NOTICE = "cancellation_with_notice"
    ELIGIBLE = "eligible"


@dataclass(frozen=True)
class Airport:
    icao: str
    country: CountryCode
    coordinates: Coordinates

    def in_scope(self) -> bool:
        return self.country in EU261_COUNTRIES


@dataclass(frozen=True)
class Carrier:
    icao: str | None
    country: CountryCode | None

    def in_scope(self) -> bool:
        return self.country is not None and self.country in EU261_COUNTRIES


@dataclass(frozen=True)
class Route:
    origin: Airport
    destination: Airport

    def distance_km(self) -> float:
        return great_circle_km(self.origin.coordinates, self.destination.coordinates)

    def intra_eu(self) -> bool:
        return self.origin.in_scope() and self.destination.in_scope()

    def band(self) -> DistanceBand:
        distance = self.distance_km()
        if distance <= SHORT_HAUL_LIMIT_KM:
            return DistanceBand.SHORT
        if self.intra_eu() or distance <= MEDIUM_HAUL_LIMIT_KM:
            return DistanceBand.MEDIUM
        return DistanceBand.LONG


@dataclass(frozen=True)
class Arrived:
    delay: timedelta


@dataclass(frozen=True)
class Cancelled:
    notice: timedelta


@dataclass(frozen=True)
class InProgress:
    """No final outcome yet (still flying, or only a predicted delay)."""


Outcome = Arrived | Cancelled | InProgress


@dataclass(frozen=True)
class FlightCase:
    route: Route
    carrier: Carrier
    outcome: Outcome
    rerouting_arrival_delay: timedelta | None = None

    def covered(self) -> bool:
        return self.route.origin.in_scope() or (
            self.route.destination.in_scope() and self.carrier.in_scope()
        )


@dataclass(frozen=True)
class Compensation:
    amount_eur: float
    reduced: bool

    @classmethod
    def none(cls) -> "Compensation":
        return cls(0, reduced=False)


@dataclass(frozen=True)
class Assessment:
    verdict: Verdict
    band: DistanceBand
    compensation: Compensation
    severe_weather: frozenset[SevereCondition] = frozenset()

    def extraordinary_circumstances_likely(self) -> bool:
        return bool(self.severe_weather)


def _verdict(case: FlightCase) -> Verdict:
    if not case.covered():
        return Verdict.NOT_COVERED
    match case.outcome:
        case Arrived(delay=delay):
            return Verdict.ELIGIBLE if delay >= DELAY_THRESHOLD else Verdict.DELAY_UNDER_THRESHOLD
        case Cancelled(notice=notice):
            return (
                Verdict.ELIGIBLE
                if notice < CANCELLATION_NOTICE
                else Verdict.CANCELLATION_WITH_NOTICE
            )
        case InProgress():
            return Verdict.PENDING


def _compensation(case: FlightCase, band: DistanceBand) -> Compensation:
    rerouted = case.rerouting_arrival_delay
    if rerouted is not None and rerouted <= band.rerouting_threshold:
        return Compensation(band.amount_eur / 2, reduced=True)
    return Compensation(band.amount_eur, reduced=False)


def assess(
    case: FlightCase, severe_weather: frozenset[SevereCondition] = frozenset()
) -> Assessment:
    band = case.route.band()
    verdict = _verdict(case)
    if verdict is not Verdict.ELIGIBLE:
        return Assessment(verdict, band, Compensation.none(), severe_weather)
    return Assessment(verdict, band, _compensation(case, band), severe_weather)
