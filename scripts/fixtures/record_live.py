"""Records live OpenSky state snapshots and METAR reports as replay fixtures.

Usage: uv run python scripts/fixtures/record_live.py [--snapshots 4] [--spacing 60]

Anonymous OpenSky access is enough for /states/all (400 credits a day; this box costs 4).
Bodies are stored exactly as received so replay exercises the real parsers.
"""

import argparse
import gzip
import json
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

from skypulse.ingestion.airports import MonitoredAirports
from skypulse.ingestion.snapshots import SNAPSHOT_FORMAT

FIXTURES = Path(__file__).resolve().parents[2] / "data" / "fixtures"
USER_AGENT = "SkyPulse/0.1 (+https://github.com/digomes87/SkyPulse)"
# Western Europe, covering every monitored European airport.
STATES_QUERY = {"lamin": 36, "lomin": -11, "lamax": 56, "lomax": 15}
METAR_HOURS = 24


def stamp() -> str:
    return datetime.now(UTC).strftime(SNAPSHOT_FORMAT)


def save(path: Path, payload: object, compress: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, separators=(",", ":")).encode()
    path.write_bytes(gzip.compress(raw, mtime=0) if compress else raw)
    print(f"wrote {path.relative_to(FIXTURES)} ({path.stat().st_size} bytes)")


def record_states(client: httpx.Client) -> None:
    response = client.get("https://opensky-network.org/api/states/all", params=STATES_QUERY)
    response.raise_for_status()
    remaining = response.headers.get("X-Rate-Limit-Remaining")
    print(f"states: {len(response.json()['states'] or [])} vectors, credits left {remaining}")
    save(FIXTURES / "opensky" / "states" / f"{stamp()}.json.gz", response.json(), compress=True)


def record_metar(client: httpx.Client) -> None:
    ids = ",".join(str(code) for code in MonitoredAirports.load().codes())
    query = {"ids": ids, "format": "json", "hours": METAR_HOURS}
    response = client.get("https://aviationweather.gov/api/data/metar", params=query)
    response.raise_for_status()
    save(FIXTURES / "metar" / f"{stamp()}.json", response.json(), compress=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshots", type=int, default=4)
    parser.add_argument("--spacing", type=float, default=60.0)
    arguments = parser.parse_args()
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=60) as client:
        record_metar(client)
        for index in range(arguments.snapshots):
            if index:
                time.sleep(arguments.spacing)
            record_states(client)


if __name__ == "__main__":
    main()
