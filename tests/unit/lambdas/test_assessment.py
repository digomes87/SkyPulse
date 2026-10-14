from datetime import timedelta

from skypulse.eu261.rules import Arrived, Cancelled, InProgress, Verdict
from skypulse.eu261.weather import SevereCondition
from skypulse.lambdas.disruption import Disruption
from tests.unit.lambdas.messages import PALMA, STANSTED, STORM, disruption


def test_arrival_delay_becomes_an_arrived_case() -> None:
    case = Disruption.from_document(disruption()).flight_case()

    assert case.outcome == Arrived(timedelta(minutes=250))
    assert case.route.origin.icao == "EIDW"
    assert case.carrier.in_scope()


def test_cancellation_notice_is_measured_from_detection() -> None:
    document = disruption(
        kind="cancellation", arrival_delay_minutes=None, detected_at="2026-10-14T05:30:00Z"
    )

    case = Disruption.from_document(document).flight_case()

    assert case.outcome == Cancelled(timedelta(hours=2))


def test_departure_delays_are_still_in_progress() -> None:
    case = Disruption.from_document(disruption(kind="departure_delay")).flight_case()

    assert case.outcome == InProgress()


def test_long_haul_delay_assesses_to_600_eur() -> None:
    assessment = Disruption.from_document(disruption()).assess()

    assert assessment.verdict is Verdict.ELIGIBLE
    assert assessment.compensation.amount_eur == 600


def test_storm_at_origin_flags_extraordinary_circumstances() -> None:
    document = disruption(
        flight_number="FR6403",
        origin=PALMA,
        destination=STANSTED,
        scheduled_departure_utc="2026-10-14T12:30:00Z",
        origin_weather=[STORM],
    )

    assessment = Disruption.from_document(document).assess()

    assert assessment.compensation.amount_eur == 250
    assert assessment.severe_weather >= {
        SevereCondition.THUNDERSTORM,
        SevereCondition.STRONG_CROSSWIND,
    }


def test_unknown_coordinates_mean_not_assessable() -> None:
    document = disruption(
        destination={"icao": "ZZZZ", "country": None, "latitude": None, "longitude": None}
    )

    assert Disruption.from_document(document).assessable() is False
