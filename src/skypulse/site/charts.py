"""Hand-rolled SVG charts for the static site.

Mark specs: bars at most 24px thick with a 4px rounded data end and a square baseline,
a 2px surface-coloured gap between stacked segments, hairline gridlines, text in ink
tokens (never the series colour). Every bar row carries a full-height hit target with a
``data-tip`` the page script turns into a tooltip; the same values are in a table view.
Colours are CSS variables (``--series-1`` ...) so light and dark themes swap in one place.
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass
from html import escape

WIDTH = 720
LABEL_WIDTH = 190
VALUE_GUTTER = 70
ROW_HEIGHT = 34
BAR_THICKNESS = 18
TOP_AXIS = 26
CORNER = 4
SEGMENT_GAP = 2
TICK_TARGET = 4


@dataclass(frozen=True)
class Series:
    name: str
    css_variable: str


@dataclass(frozen=True)
class BarRow:
    label: str
    values: tuple[float, ...]
    tip: str
    monospace_label: bool = False

    def total(self) -> float:
        return sum(self.values)


def nice_ticks(maximum: float) -> list[float]:
    """Round tick values from 0 to just past ``maximum`` (1, 2, 2.5, 5 x 10^n steps)."""
    if maximum <= 0:
        return [0.0, 1.0]
    raw_step = maximum / TICK_TARGET
    magnitude = 10 ** math.floor(math.log10(raw_step))
    step = next(m * magnitude for m in (1, 2, 2.5, 5, 10) if m * magnitude >= raw_step)
    count = math.ceil(maximum / step)
    return [round(step * index, 10) for index in range(count + 1)]


def _rounded_end_bar(x: float, y: float, width: float, height: float, rounded: bool) -> str:
    """Rectangle with a rounded right end (the data end) and a square left end."""
    if width <= 0:
        return ""
    radius = min(CORNER, width / 2, height / 2) if rounded else 0
    return (
        f"M{x:.1f},{y:.1f} h{width - radius:.1f} "
        f"a{radius},{radius} 0 0 1 {radius},{radius} v{height - 2 * radius:.1f} "
        f"a{radius},{radius} 0 0 1 {-radius},{radius} h{-(width - radius):.1f} z"
    )


def _axis(ticks: list[float], scale: float, height: float, unit_format: str) -> str:
    parts = []
    for tick in ticks:
        x = LABEL_WIDTH + tick * scale
        parts.append(
            f'<line class="grid" x1="{x:.1f}" x2="{x:.1f}" y1="{TOP_AXIS - 6}" y2="{height}"/>'
            f'<text class="tick" x="{x:.1f}" y="{TOP_AXIS - 12}" text-anchor="middle">'
            f"{escape(unit_format.format(tick))}</text>"
        )
    return "".join(parts)


def _segments(row: BarRow, series: Sequence[Series], y: float, scale: float) -> str:
    parts = []
    x = float(LABEL_WIDTH)
    filled = [index for index, value in enumerate(row.values) if value > 0]
    for index, value in enumerate(row.values):
        if value <= 0:
            continue
        width = value * scale - (SEGMENT_GAP if index != filled[-1] else 0)
        path = _rounded_end_bar(x, y, width, BAR_THICKNESS, rounded=index == filled[-1])
        parts.append(f'<path d="{path}" fill="var({series[index].css_variable})"/>')
        x += value * scale
    return "".join(parts)


def horizontal_bars(
    rows: Sequence[BarRow],
    series: Sequence[Series],
    title: str,
    unit_format: str = "{:,.0f}",
) -> str:
    """Horizontal (optionally stacked) bar chart; one row per category, value at the tip."""
    height = TOP_AXIS + ROW_HEIGHT * len(rows) + 8
    maximum = max((row.total() for row in rows), default=0.0)
    ticks = nice_ticks(maximum)
    scale = (WIDTH - LABEL_WIDTH - VALUE_GUTTER) / ticks[-1]
    body = [_axis(ticks, scale, height, unit_format)]
    for index, row in enumerate(rows):
        top = TOP_AXIS + index * ROW_HEIGHT
        bar_y = top + (ROW_HEIGHT - BAR_THICKNESS) / 2
        label_class = "label mono" if row.monospace_label else "label"
        tip_x = LABEL_WIDTH + row.total() * scale + 8
        body.append(
            f'<g class="row" tabindex="0" data-tip="{escape(row.tip)}">'
            f'<rect class="hit" x="0" y="{top}" width="{WIDTH}" height="{ROW_HEIGHT}"/>'
            f'<text class="{label_class}" x="{LABEL_WIDTH - 12}" y="{top + ROW_HEIGHT / 2}" '
            f'text-anchor="end" dominant-baseline="middle">{escape(row.label)}</text>'
            f"{_segments(row, series, bar_y, scale)}"
            f'<text class="value" x="{tip_x:.1f}" y="{top + ROW_HEIGHT / 2}" '
            f'dominant-baseline="middle">{escape(unit_format.format(row.total()))}</text>'
            "</g>"
        )
    return (
        f'<svg class="chart" viewBox="0 0 {WIDTH} {height}" role="img" '
        f'aria-label="{escape(title)}">{"".join(body)}</svg>'
    )


@dataclass(frozen=True)
class Point:
    longitude: float
    latitude: float


@dataclass(frozen=True)
class Projection:
    """Equirectangular projection over a fixed box, longitudes shrunk by cos(mid-latitude)."""

    west: float = -11.5
    east: float = 15.5
    south: float = 35.5
    north: float = 56.5

    def height(self) -> float:
        aspect = (self.north - self.south) / (
            (self.east - self.west) * math.cos(math.radians((self.north + self.south) / 2))
        )
        return WIDTH * aspect

    def project(self, point: Point) -> tuple[float, float] | None:
        inside = (
            self.west <= point.longitude <= self.east and self.south <= point.latitude <= self.north
        )
        if not inside:
            return None
        x = (point.longitude - self.west) / (self.east - self.west) * WIDTH
        y = (self.north - point.latitude) / (self.north - self.south) * self.height()
        return x, y


def _aircraft_dot(point: Point, projection: Projection) -> str:
    projected = projection.project(point)
    if projected is None:
        return ""
    x, y = projected
    return f'<circle class="aircraft" cx="{x:.1f}" cy="{y:.1f}" r="1.4"/>'


NEIGHBOUR_DISTANCE = 60.0
LABEL_OFFSET = 9.0


@dataclass(frozen=True)
class PlacedHub:
    """A hub marker; its label sits away from the nearest neighbouring hub."""

    icao: str
    x: float
    y: float
    label_left: bool = False
    label_below: bool = False

    def svg(self) -> str:
        label_x = self.x - LABEL_OFFSET if self.label_left else self.x + LABEL_OFFSET
        label_y = self.y + 2 * LABEL_OFFSET if self.label_below else self.y - LABEL_OFFSET
        anchor = "end" if self.label_left else "start"
        return (
            f'<g class="hub" tabindex="0" data-tip="{escape(self.icao)}">'
            f'<circle cx="{self.x:.1f}" cy="{self.y:.1f}" r="12" class="hit"/>'
            f'<circle cx="{self.x:.1f}" cy="{self.y:.1f}" r="5" class="hub-dot"/>'
            f'<text class="label mono" x="{label_x:.1f}" y="{label_y:.1f}" '
            f'text-anchor="{anchor}">{escape(self.icao)}</text></g>'
        )

    def away_from(self, others: Sequence["PlacedHub"]) -> "PlacedHub":
        near = [
            o
            for o in others
            if o is not self and math.dist((o.x, o.y), (self.x, self.y)) < NEIGHBOUR_DISTANCE
        ]
        if not near:
            return self
        closest = min(near, key=lambda o: math.dist((o.x, o.y), (self.x, self.y)))
        return PlacedHub(self.icao, self.x, self.y, closest.x > self.x, closest.y < self.y)


def place_hubs(hubs: Sequence[tuple[str, Point]], projection: Projection) -> list[PlacedHub]:
    projected = [(icao, projection.project(point)) for icao, point in hubs]
    markers = [PlacedHub(icao, *position) for icao, position in projected if position is not None]
    return [marker.away_from(markers) for marker in markers]


def dot_map(
    aircraft: Sequence[Point], hubs: Sequence[tuple[str, Point]], title: str, projection: Projection
) -> str:
    """Density map of aircraft positions with labelled hub airports on top.

    Aircraft dots are deliberately tiny and semi-transparent: with thousands of them the
    density is the message, so they carry no hover; hubs are full-size markers with a
    surface ring and an ICAO label.
    """
    height = f"{projection.height():.0f}"
    dots = "".join(_aircraft_dot(point, projection) for point in aircraft)
    markers = "".join(hub.svg() for hub in place_hubs(hubs, projection))
    surface = f'<rect class="map-surface" width="{WIDTH}" height="{height}"/>'
    return (
        f'<svg class="chart map" viewBox="0 0 {WIDTH} {height}" role="img" '
        f'aria-label="{escape(title)}">{surface}{dots}{markers}</svg>'
    )
