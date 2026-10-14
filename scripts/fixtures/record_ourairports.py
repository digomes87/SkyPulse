"""Records a subset of the OurAirports data set as replay fixtures.

Usage: uv run python scripts/fixtures/record_ourairports.py

Keeps the raw CSV format so the reference loader parses the same file as in production.
Subset: large and medium airports in Europe plus the monitored non-European ones, their
runways, and the full country list.
"""

import csv
import io
from pathlib import Path

import httpx

from skypulse.ingestion.airports import MonitoredAirports

BASE_URL = "https://davidmegginson.github.io/ourairports-data"
OUTPUT = Path(__file__).resolve().parents[2] / "data" / "fixtures" / "ourairports"
KEPT_TYPES = {"large_airport", "medium_airport"}


def download(client: httpx.Client, name: str) -> tuple[list[str], list[dict[str, str]]]:
    response = client.get(f"{BASE_URL}/{name}.csv")
    response.raise_for_status()
    reader = csv.DictReader(io.StringIO(response.text))
    return list(reader.fieldnames or []), list(reader)


def write(name: str, header: list[str], rows: list[dict[str, str]]) -> None:
    path = OUTPUT / f"{name}.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {path.name}: {len(rows)} rows")


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    monitored = {str(code) for code in MonitoredAirports.load().codes()}
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        airport_header, airports = download(client, "airports")
        runway_header, runways = download(client, "runways")
        country_header, countries = download(client, "countries")
    kept = [
        row
        for row in airports
        if row["icao_code"]
        and (
            (row["continent"] == "EU" and row["type"] in KEPT_TYPES)
            or row["icao_code"] in monitored
        )
    ]
    idents = {row["ident"] for row in kept}
    write("airports", airport_header, kept)
    write("runways", runway_header, [row for row in runways if row["airport_ident"] in idents])
    write("countries", country_header, countries)


if __name__ == "__main__":
    main()
