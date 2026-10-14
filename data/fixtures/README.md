# Replay fixtures

Everything SkyPulse needs to run offline: `make demo`, CI integration tests and local
development all read these files through the production clients and parsers.

| Folder | Content | Origin |
|--------|---------|--------|
| `opensky/states/` | Raw `/states/all` bodies (gzip), Western Europe box | **Recorded** from the live OpenSky API on 2026-10-14 (`scripts/fixtures/record_live.py`) |
| `metar/20261014T1016Z.json` | Raw `/api/data/metar` JSON, last 24 h for every monitored airport | **Recorded** from aviationweather.gov on 2026-10-14 |
| `metar/20261014T1300Z.json` | The recorded reports plus three SPECI reports of a thunderstorm at LEPA | **Synthetic scenario** (`scripts/fixtures/add_weather_scenario.py`), station name tagged `SYNTHETIC SCENARIO` |
| `aerodatabox/<ICAO>/` | Hourly FIDS snapshots (withLeg=true) from 05:00Z to 22:00Z | **Synthetic** (`scripts/fixtures/generate_schedules.py`, seed 261) |

## Why synthetic schedules

Scheduled vs actual times come from AeroDataBox, which needs a paid key and returns
different data on every call. A deterministic synthetic day in the exact FIDS format keeps
the demo reproducible and lets tests rely on known outcomes. Three scenarios are pinned:

| Flight | Route | Scenario |
|--------|-------|----------|
| EI105 | EIDW-KJFK | 4h10 arrival delay on a long-haul flight (EUR 600 band) |
| FR6403 | LEPA-EGSS | 3h35 delay during a thunderstorm (eligible, but weather exemption flag) |
| FR1234 | EIDW-LEMD | Cancelled on the day of departure |

The other 70 flights draw delays from a fixed distribution (60% under 15 min, 3%
cancelled).

## File naming

`<YYYYMMDDTHHMMZ>.json[.gz]`: the UTC minute the snapshot represents. Replay serves the
newest snapshot that is not later than the replay clock (see `skypulse.ingestion.snapshots`).

## Regenerating

```bash
uv run python scripts/fixtures/record_live.py        # needs network, ~4 OpenSky credits per snapshot
uv run python scripts/fixtures/add_weather_scenario.py
uv run python scripts/fixtures/generate_schedules.py
```

Recorded data is published by the OpenSky Network and NOAA's Aviation Weather Center; see
`docs/data-sources.md` for their terms.
