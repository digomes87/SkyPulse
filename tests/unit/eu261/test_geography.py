import pytest
from hypothesis import given
from hypothesis import strategies as st

from skypulse.eu261.geography import EU261_COUNTRIES, Coordinates, CountryCode, great_circle_km

latitudes = st.floats(min_value=-90, max_value=90, allow_nan=False)
longitudes = st.floats(min_value=-180, max_value=180, allow_nan=False)
points = st.builds(Coordinates, latitudes, longitudes)

DUBLIN = Coordinates(53.4213, -6.2701)
MADRID = Coordinates(40.4719, -3.5626)
NEW_YORK = Coordinates(40.6413, -73.7781)


def test_known_distances() -> None:
    assert great_circle_km(DUBLIN, MADRID) == pytest.approx(1452, abs=5)
    assert great_circle_km(DUBLIN, NEW_YORK) == pytest.approx(5103, abs=10)


@given(points, points)
def test_distance_is_symmetric_and_bounded(a: Coordinates, b: Coordinates) -> None:
    distance = great_circle_km(a, b)

    assert distance == pytest.approx(great_circle_km(b, a), abs=1e-6)
    assert 0 <= distance <= 20_016


@given(points)
def test_distance_to_itself_is_zero(a: Coordinates) -> None:
    assert great_circle_km(a, a) == pytest.approx(0, abs=1e-6)


@given(points, points, points)
def test_triangle_inequality(a: Coordinates, b: Coordinates, c: Coordinates) -> None:
    assert great_circle_km(a, c) <= great_circle_km(a, b) + great_circle_km(b, c) + 1e-6


def test_coordinates_are_validated() -> None:
    with pytest.raises(ValueError, match="latitude"):
        Coordinates(90.5, 0)


@pytest.mark.parametrize("code", ["IE", "ES", "GB", "CH", "NO", "IS", "LI", "GP"])
def test_regulation_scope_covers_eu_eea_uk_and_switzerland(code: str) -> None:
    assert CountryCode(code) in EU261_COUNTRIES


@pytest.mark.parametrize("code", ["US", "TR", "MA"])
def test_other_countries_are_outside(code: str) -> None:
    assert CountryCode(code) not in EU261_COUNTRIES


def test_country_codes_are_two_upper_letters() -> None:
    with pytest.raises(ValueError, match="ISO"):
        CountryCode("ESP")
