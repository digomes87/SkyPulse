from datetime import timedelta

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from skypulse.eu261.geography import Coordinates, CountryCode
from skypulse.eu261.rules import (
    Airport,
    Arrived,
    Cancelled,
    Carrier,
    DistanceBand,
    FlightCase,
    InProgress,
    Outcome,
    Route,
    Verdict,
    assess,
)
from skypulse.eu261.weather import SevereCondition

DUBLIN = Airport("EIDW", CountryCode("IE"), Coordinates(53.4213, -6.2701))
MADRID = Airport("LEMD", CountryCode("ES"), Coordinates(40.4719, -3.5626))
LISBON = Airport("LPPT", CountryCode("PT"), Coordinates(38.7742, -9.1342))
HELSINKI = Airport("EFHK", CountryCode("FI"), Coordinates(60.3172, 24.9633))
NEW_YORK = Airport("KJFK", CountryCode("US"), Coordinates(40.6413, -73.7781))
ISTANBUL = Airport("LTFM", CountryCode("TR"), Coordinates(41.2753, 28.7519))
CAIRO = Airport("HECA", CountryCode("EG"), Coordinates(30.1219, 31.4056))

RYANAIR = Carrier("RYR", CountryCode("IE"))
DELTA = Carrier("DAL", CountryCode("US"))

FOUR_HOURS = Arrived(timedelta(hours=4))


def case(
    origin: Airport, destination: Airport, carrier: Carrier = RYANAIR, outcome: Outcome = FOUR_HOURS
) -> FlightCase:
    return FlightCase(Route(origin, destination), carrier, outcome)


@pytest.mark.parametrize(
    ("origin", "destination", "band"),
    [
        (DUBLIN, MADRID, DistanceBand.SHORT),  # ~1450 km
        (LISBON, HELSINKI, DistanceBand.MEDIUM),  # ~3360 km, intra-EU
        (DUBLIN, ISTANBUL, DistanceBand.MEDIUM),  # ~2990 km, extra-EU
        (DUBLIN, NEW_YORK, DistanceBand.LONG),  # ~5100 km
    ],
)
def test_distance_bands(origin: Airport, destination: Airport, band: DistanceBand) -> None:
    assert assess(case(origin, destination)).band is band


def test_long_delay_is_eligible_for_the_band_amount() -> None:
    assessment = assess(case(DUBLIN, NEW_YORK))

    assert assessment.verdict is Verdict.ELIGIBLE
    assert assessment.compensation.amount_eur == 600


def test_departures_from_scope_are_covered_whatever_the_carrier() -> None:
    assert assess(case(DUBLIN, NEW_YORK, carrier=DELTA)).verdict is Verdict.ELIGIBLE


def test_arrivals_into_scope_need_an_eu_carrier() -> None:
    assert assess(case(NEW_YORK, DUBLIN, carrier=DELTA)).verdict is Verdict.NOT_COVERED
    assert assess(case(NEW_YORK, DUBLIN, carrier=RYANAIR)).verdict is Verdict.ELIGIBLE


def test_flights_entirely_outside_scope_are_not_covered() -> None:
    assert assess(case(ISTANBUL, CAIRO)).verdict is Verdict.NOT_COVERED


@pytest.mark.parametrize(
    ("notice", "verdict"),
    [(timedelta(days=2), Verdict.ELIGIBLE), (timedelta(days=14), Verdict.CANCELLATION_WITH_NOTICE)],
)
def test_cancellations_depend_on_notice(notice: timedelta, verdict: Verdict) -> None:
    assert assess(case(DUBLIN, MADRID, outcome=Cancelled(notice))).verdict is verdict


def test_flights_without_an_outcome_are_pending() -> None:
    assessment = assess(case(DUBLIN, MADRID, outcome=InProgress()))

    assert assessment.verdict is Verdict.PENDING
    assert assessment.compensation.amount_eur == 0


def test_rerouting_within_the_threshold_halves_the_amount() -> None:
    rerouted = FlightCase(
        Route(DUBLIN, NEW_YORK), RYANAIR, Cancelled(timedelta(days=1)), timedelta(hours=4)
    )

    compensation = assess(rerouted).compensation

    assert compensation.amount_eur == 300
    assert compensation.reduced


def test_severe_weather_is_flagged_without_changing_the_amount() -> None:
    assessment = assess(case(DUBLIN, MADRID), frozenset({SevereCondition.THUNDERSTORM}))

    assert assessment.extraordinary_circumstances_likely()
    assert assessment.compensation.amount_eur == 250


# --- properties ------------------------------------------------------------------------

minutes = st.integers(min_value=0, max_value=24 * 60)
latitudes = st.floats(min_value=-60, max_value=70, allow_nan=False)
longitudes = st.floats(min_value=-180, max_value=180, allow_nan=False)


@st.composite
def airports(draw: st.DrawFn) -> Airport:
    country = draw(st.sampled_from(["IE", "ES", "DE", "US", "TR", "BR"]))
    return Airport("XXXX", CountryCode(country), Coordinates(draw(latitudes), draw(longitudes)))


@given(airports(), airports(), minutes)
def test_three_hours_is_the_exact_delay_threshold(
    origin: Airport, destination: Airport, delay: int
) -> None:
    flight = case(origin, destination, outcome=Arrived(timedelta(minutes=delay)))
    assume(flight.covered())

    verdict = assess(flight).verdict

    assert (verdict is Verdict.ELIGIBLE) == (delay >= 180)


@given(airports(), airports(), minutes, st.one_of(st.none(), minutes))
def test_amount_is_always_a_regulation_amount(
    origin: Airport, destination: Airport, delay: int, rerouting: int | None
) -> None:
    rerouting_delay = None if rerouting is None else timedelta(minutes=rerouting)
    flight = FlightCase(
        Route(origin, destination), RYANAIR, Arrived(timedelta(minutes=delay)), rerouting_delay
    )

    assert assess(flight).compensation.amount_eur in {0, 125, 200, 250, 300, 400, 600}


def expected_band(route: Route) -> DistanceBand:
    distance = route.distance_km()
    if distance <= 1500:
        return DistanceBand.SHORT
    if route.intra_eu() or distance <= 3500:
        return DistanceBand.MEDIUM
    return DistanceBand.LONG


@given(airports(), airports())
def test_band_follows_distance_and_scope(origin: Airport, destination: Airport) -> None:
    route = Route(origin, destination)

    assert route.band() is expected_band(route)


def equator_route(longitude: float) -> Route:
    origin = Airport("AAAA", CountryCode("BR"), Coordinates(0, 0))
    return Route(origin, Airport("BBBB", CountryCode("US"), Coordinates(0, longitude)))


@given(st.floats(min_value=0, max_value=179), st.floats(min_value=0, max_value=179))
def test_amount_never_drops_as_distance_grows(first: float, second: float) -> None:
    near, far = sorted((first, second))

    assert equator_route(near).band().amount_eur <= equator_route(far).band().amount_eur
