"""Adds a synthetic thunderstorm at Palma (LEPA) on top of the recorded METAR fixtures.

Usage: uv run python scripts/fixtures/add_weather_scenario.py

The recorded reports end when they were captured, and real weather rarely lines up with a
demo. This writes a new snapshot at 13:00Z that contains every recorded report plus SPECI
reports for a thunderstorm over LEPA around the scheduled departure of the pinned FR6403
scenario (see generate_schedules.py). Synthetic reports are tagged in the station name.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

from skypulse.ingestion.snapshots import SnapshotFolder

METAR_DIR = Path(__file__).resolve().parents[2] / "data" / "fixtures" / "metar"
SCENARIO_SNAPSHOT = datetime(2026, 10, 14, 13, 0, tzinfo=UTC)
SYNTHETIC_NAME = "Palma De Mallorca, IB, ES (SYNTHETIC SCENARIO)"


def speci(hour: int, minute: int, raw_weather: str, **fields: object) -> dict[str, object]:
    observed = datetime(2026, 10, 14, hour, minute, tzinfo=UTC)
    stamp = observed.strftime("%d%H%MZ")
    wind = f"{fields['wdir']:03d}{fields['wspd']:02d}G{fields['wgst']:02d}KT"
    visibility = f"{fields['visibility_m']:04d}"
    raw = f"SPECI LEPA {stamp} {wind} {visibility} {raw_weather} BKN015CB 21/19 Q1006"
    return {
        "icaoId": "LEPA",
        "obsTime": int(observed.timestamp()),
        "reportTime": observed.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "metarType": "SPECI",
        "temp": 21,
        "dewp": 19,
        "wdir": fields["wdir"],
        "wspd": fields["wspd"],
        "wgst": fields["wgst"],
        "visib": round(int(str(fields["visibility_m"])) / 1609.344, 2),
        "wxString": raw_weather,
        "altim": 1006,
        "rawOb": raw,
        "name": SYNTHETIC_NAME,
        "fltCat": "IFR",
    }


SCENARIO = [
    speci(11, 50, "+TSRA", wdir=230, wspd=24, wgst=41, visibility_m=1500),
    speci(12, 20, "+TSRA", wdir=240, wspd=28, wgst=45, visibility_m=1200),
    speci(12, 50, "TSRA", wdir=250, wspd=22, wgst=36, visibility_m=2500),
]


def main() -> None:
    folder = SnapshotFolder(METAR_DIR)
    recorded = folder.latest_at(SCENARIO_SNAPSHOT.replace(hour=12, minute=59))
    if recorded is None:
        raise SystemExit("record live METAR first (scripts/fixtures/record_live.py)")
    reports = [item for item in recorded.load() if item.get("name") != SYNTHETIC_NAME]
    target = METAR_DIR / SCENARIO_SNAPSHOT.strftime("%Y%m%dT%H%MZ.json")
    target.write_text(json.dumps([*SCENARIO, *reports], separators=(",", ":")))
    print(f"wrote {target.name}: {len(reports)} recorded + {len(SCENARIO)} synthetic reports")


if __name__ == "__main__":
    main()
