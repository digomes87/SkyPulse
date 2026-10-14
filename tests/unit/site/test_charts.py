import re

import pytest

from skypulse.site.charts import (
    BarRow,
    Point,
    Projection,
    Series,
    dot_map,
    horizontal_bars,
    nice_ticks,
    place_hubs,
)

ONE = Series("One", "--series-1")
TWO = Series("Two", "--series-2")


@pytest.mark.parametrize(
    ("maximum", "ticks"),
    [
        (900, [0, 250, 500, 750, 1000]),
        (75, [0, 20, 40, 60, 80]),
        (47, [0, 20, 40, 60]),
        (0, [0, 1]),
    ],
)
def test_ticks_are_round_and_cover_the_maximum(maximum: float, ticks: list[float]) -> None:
    assert nice_ticks(maximum) == ticks


def test_stacked_bars_draw_one_path_per_non_zero_segment() -> None:
    rows = [BarRow("A", (650, 250), "a"), BarRow("B", (600, 0), "b"), BarRow("C", (0, 0), "c")]

    svg = horizontal_bars(rows, (ONE, TWO), "Exposure")

    assert svg.count("<path") == 3
    assert svg.count('class="row"') == 3
    assert 'aria-label="Exposure"' in svg


def test_only_the_data_end_of_a_bar_is_rounded() -> None:
    svg = horizontal_bars([BarRow("A", (650, 250), "a")], (ONE, TWO), "x")

    paths = re.findall(r'd="([^"]+)"', svg)
    assert " a0,0 " in f" {paths[0]} "
    assert " a4,4 " in f" {paths[1]} "


def test_tooltips_and_labels_are_escaped() -> None:
    svg = horizontal_bars([BarRow("<b>", (1,), 'say "hi"')], (ONE,), "x")

    assert "<b>" not in svg
    assert "&quot;hi&quot;" in svg


def test_projection_keeps_points_inside_the_box() -> None:
    projection = Projection()

    assert projection.project(Point(0, 50)) is not None
    assert projection.project(Point(40, 50)) is None


def test_crowded_hub_labels_point_away_from_each_other() -> None:
    hubs = [
        ("EGLL", Point(-0.46, 51.47)),
        ("EGSS", Point(0.23, 51.88)),
        ("LEMD", Point(-3.56, 40.47)),
    ]

    placed = {hub.icao: hub for hub in place_hubs(hubs, Projection())}

    assert placed["EGLL"].label_left
    assert placed["EGLL"].label_below
    assert not placed["EGSS"].label_left
    assert not placed["EGSS"].label_below
    assert not placed["LEMD"].label_left


def test_dot_map_draws_aircraft_and_hubs() -> None:
    svg = dot_map(
        [Point(0, 50), Point(50, 50)], [("EIDW", Point(-6.27, 53.42))], "Airspace", Projection()
    )

    assert svg.count('class="aircraft"') == 1
    assert "EIDW" in svg
