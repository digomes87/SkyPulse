from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st

from skypulse.eu261.weather import (
    SevereCondition,
    WeatherObservation,
    WeatherPolicy,
    Wind,
    crosswind_kt,
    severe_conditions,
    severe_weather_around,
)

T0 = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
CALM = Wind(200, 8, None)
POLICY = WeatherPolicy()


def observation(
    weather: str | None = None,
    visibility_m: int | None = 9999,
    wind: Wind = CALM,
    at: datetime = T0,
) -> WeatherObservation:
    return WeatherObservation(at, weather, visibility_m, wind)


def test_calm_weather_is_not_severe() -> None:
    assert severe_conditions(observation(), POLICY) == frozenset()


@pytest.mark.parametrize(
    ("weather", "expected"),
    [
        ("+TSRA", SevereCondition.THUNDERSTORM),
        ("VCTS", SevereCondition.THUNDERSTORM),
        ("FZRA", SevereCondition.FREEZING_PRECIPITATION),
        ("-FZDZ BR", SevereCondition.FREEZING_PRECIPITATION),
    ],
)
def test_present_weather_groups(weather: str, expected: SevereCondition) -> None:
    assert expected in severe_conditions(observation(weather), POLICY)


def test_freezing_fog_is_not_freezing_precipitation() -> None:
    assert SevereCondition.FREEZING_PRECIPITATION not in severe_conditions(
        observation("FZFG"), POLICY
    )


def test_low_visibility_uses_the_configured_threshold() -> None:
    assert SevereCondition.LOW_VISIBILITY in severe_conditions(
        observation(visibility_m=500), POLICY
    )
    relaxed = WeatherPolicy(minimum_visibility_m=400)
    assert severe_conditions(observation(visibility_m=500), relaxed) == frozenset()


def test_crosswind_without_runway_data_assumes_the_worst_case() -> None:
    gusty = Wind(270, 25, 40)

    assert crosswind_kt(gusty, runway_headings=()) == 40
    assert SevereCondition.STRONG_CROSSWIND in severe_conditions(observation(wind=gusty), POLICY)


def test_crosswind_uses_the_best_aligned_runway() -> None:
    along_runway = Wind(280, 30, 45)

    assert crosswind_kt(along_runway, runway_headings=(100, 280)) == pytest.approx(0, abs=1e-9)
    assert crosswind_kt(along_runway, runway_headings=(300,)) == pytest.approx(15.39, abs=0.01)


def test_variable_wind_counts_as_worst_case() -> None:
    assert crosswind_kt(Wind(None, 20, None), runway_headings=(90,)) == 20


@given(
    st.integers(0, 360), st.integers(0, 80), st.lists(st.integers(0, 359), min_size=1, max_size=4)
)
def test_crosswind_never_exceeds_the_wind(direction: int, speed: int, runways: list[int]) -> None:
    value = crosswind_kt(Wind(direction, speed, None), tuple(runways))

    assert 0 <= value <= speed + 1e-9


def test_only_observations_near_the_scheduled_time_count() -> None:
    storm_long_before = observation("+TSRA", at=T0 - timedelta(hours=3))
    storm_just_before = observation("+TSRA", at=T0 - timedelta(minutes=40))

    assert severe_weather_around([storm_long_before], T0, POLICY) == frozenset()
    assert severe_weather_around([storm_just_before], T0, POLICY) == {SevereCondition.THUNDERSTORM}
