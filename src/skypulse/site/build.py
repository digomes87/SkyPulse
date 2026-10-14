"""Static site build: ``python -m skypulse.site.build --gold DB --dbt-target DIR --out DIR``."""

import argparse
import shutil
from datetime import UTC, datetime
from pathlib import Path

from jinja2 import Environment, PackageLoader, StrictUndefined, select_autoescape

from skypulse.site.charts import BarRow, Point, Projection, Series, dot_map, horizontal_bars
from skypulse.site.data import Gold, load_gold

DBT_DOCS_FILES = ("index.html", "manifest.json", "catalog.json")
REPOSITORY_URL = "https://github.com/digomes87/SkyPulse"

EXPECTED = Series("Expected payout", "--series-1")
EXEMPT = Series("Likely weather-exempt", "--series-2")
DELAY = Series("Average departure delay", "--series-1")
FLIGHTS = Series("Flights", "--series-1")

BAND_LABELS = {
    "on_time": "On time (< 15 min)",
    "15_to_59": "15-59 min late",
    "60_to_179": "1-3 h late",
    "180_plus": "3 h or more (EU261)",
    "cancelled": "Cancelled",
}


def euros(amount: float) -> str:
    return f"€{amount:,.0f}"


def exposure_chart(gold: Gold) -> str:
    rows = [
        BarRow(
            f"{airline.airline_name} · {airline.airline_icao}",
            (airline.expected_payout_eur, airline.likely_exempt_eur),
            f"{airline.airline_name}: {euros(airline.expected_payout_eur)} expected, "
            f"{euros(airline.likely_exempt_eur)} likely exempt, {airline.flights} flights",
        )
        for airline in gold.airlines
    ]
    return horizontal_bars(rows, (EXPECTED, EXEMPT), "EU261 exposure by airline", "€{:,.0f}")


def airport_delay_chart(gold: Gold) -> str:
    rows = [
        BarRow(
            airport.airport_icao,
            (max(airport.avg_departure_delay_minutes, 0.0),),
            f"{airport.airport_name}: {airport.avg_departure_delay_minutes:.0f} min average "
            f"departure delay over {airport.departures} departures",
            monospace_label=True,
        )
        for airport in gold.airports
    ]
    return horizontal_bars(rows, (DELAY,), "Average departure delay by airport", "{:,.0f} min")


def delay_band_chart(gold: Gold) -> str:
    rows = [
        BarRow(
            BAND_LABELS[band.band],
            (float(band.flights),),
            f"{BAND_LABELS[band.band]}: {band.flights} flights",
        )
        for band in gold.delay_bands
    ]
    return horizontal_bars(rows, (FLIGHTS,), "Flights by arrival delay", "{:,.0f}")


def airspace_map(gold: Gold) -> str:
    aircraft = [Point(position.longitude, position.latitude) for position in gold.positions]
    hubs = [(hub.icao, Point(hub.longitude, hub.latitude)) for hub in gold.hubs]
    return dot_map(aircraft, hubs, "Aircraft positions over Western Europe", Projection())


def render(gold: Gold, out: Path) -> None:
    environment = Environment(
        loader=PackageLoader("skypulse.site", "templates"),
        autoescape=select_autoescape(["html", "j2"]),
        undefined=StrictUndefined,
    )
    environment.filters["euros"] = euros
    context = {
        "gold": gold,
        "repository": REPOSITORY_URL,
        "built_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "charts": {
            "exposure": exposure_chart(gold),
            "airports": airport_delay_chart(gold),
            "bands": delay_band_chart(gold),
            "map": airspace_map(gold),
        },
        "series": {"exposure": (EXPECTED, EXEMPT)},
        "band_labels": BAND_LABELS,
    }
    pages = {"index.html": "index.html.j2", "insights/index.html": "insights.html.j2"}
    for target, template in pages.items():
        path = out / target
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            environment.get_template(template).render(**context, depth=target.count("/"))
        )


def copy_static(out: Path) -> None:
    source = Path(__file__).parent / "static"
    shutil.copytree(source, out / "static", dirs_exist_ok=True)


def copy_dbt_docs(dbt_target: Path, out: Path) -> None:
    docs = out / "dbt"
    docs.mkdir(parents=True, exist_ok=True)
    for name in DBT_DOCS_FILES:
        shutil.copy2(dbt_target / name, docs / name)


def build(gold_database: Path, dbt_target: Path | None, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    render(load_gold(gold_database), out)
    copy_static(out)
    (out / ".nojekyll").write_text("")
    if dbt_target is not None:
        copy_dbt_docs(dbt_target, out)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold", type=Path, default=Path("dbt/target/skypulse.duckdb"))
    parser.add_argument("--dbt-target", type=Path, default=None, help="dbt target dir with docs")
    parser.add_argument("--out", type=Path, default=Path("public"))
    arguments = parser.parse_args(argv)
    build(arguments.gold, arguments.dbt_target, arguments.out)


if __name__ == "__main__":
    main()
