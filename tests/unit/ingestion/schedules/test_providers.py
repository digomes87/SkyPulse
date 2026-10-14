import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from pydantic import SecretStr

from skypulse.common.aviation import AirportIcao, TimeWindow
from skypulse.common.clock import FrozenClock
from skypulse.common.http import RetryingHttp, RetryPolicy
from skypulse.ingestion.airports import MonitoredAirport
from skypulse.ingestion.schedules.aerodatabox import AeroDataBoxProvider
from skypulse.ingestion.schedules.replay import ReplayProvider
from tests.unit.ingestion.schedules.fids_sample import arrival, departure, fids

DUBLIN = MonitoredAirport(AirportIcao("EIDW"), ZoneInfo("Europe/Dublin"))
MORNING = TimeWindow(
    datetime(2026, 10, 14, 5, 0, tzinfo=UTC), datetime(2026, 10, 14, 11, 0, tzinfo=UTC)
)
NOW = datetime(2026, 10, 14, 8, 0, tzinfo=UTC)


def provider_for(transport: httpx.MockTransport) -> AeroDataBoxProvider:
    http = RetryingHttp(httpx.AsyncClient(transport=transport), RetryPolicy(max_attempts=1))
    return AeroDataBoxProvider(http, SecretStr("key-123"), FrozenClock(NOW))


async def test_aerodatabox_requests_local_time_windows_with_rapidapi_headers() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=fids(departure()))

    batch = await provider_for(httpx.MockTransport(handler)).flights(DUBLIN, MORNING)

    assert len(batch) == 1
    request = seen[0]
    assert request.url.path == "/flights/airports/icao/EIDW/2026-10-14T06:00/2026-10-14T12:00"
    assert request.url.params["withLeg"] == "true"
    assert request.headers["X-RapidAPI-Key"] == "key-123"
    assert request.headers["X-RapidAPI-Host"] == "aerodatabox.p.rapidapi.com"


async def test_aerodatabox_splits_long_windows_into_twelve_hour_calls() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    day = TimeWindow(MORNING.start, MORNING.start + timedelta(hours=24))

    batch = await provider_for(httpx.MockTransport(handler)).flights(DUBLIN, day)

    assert len(seen) == 2
    assert len(batch) == 0


def write_snapshot(root: Path, taken_at: str, payload: dict[str, object]) -> None:
    folder = root / "EIDW"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{taken_at}.json").write_text(json.dumps(payload))


async def test_replay_serves_the_latest_snapshot_not_after_the_clock(tmp_path: Path) -> None:
    write_snapshot(tmp_path, "20261014T0600Z", fids(departure(status="Boarding")))
    write_snapshot(tmp_path, "20261014T0730Z", fids(departure(status="Departed")))
    write_snapshot(tmp_path, "20261014T0900Z", fids(departure(status="Arrived")))

    batch = await ReplayProvider(tmp_path, FrozenClock(NOW)).flights(DUBLIN, MORNING)

    flight = next(iter(batch))
    assert flight.data["status"] == "Departed"
    assert flight.data["observed_at"] == "2026-10-14T07:30:00Z"


async def test_replay_keeps_only_flights_inside_the_window(tmp_path: Path) -> None:
    write_snapshot(tmp_path, "20261014T0600Z", fids(departure(), arrivals=(arrival(),)))
    early = TimeWindow(MORNING.start, MORNING.start + timedelta(hours=3))

    batch = await ReplayProvider(tmp_path, FrozenClock(NOW)).flights(DUBLIN, early)

    assert [flight.data["flight_number"] for flight in batch] == ["FR7241"]


async def test_replay_without_snapshots_is_empty(tmp_path: Path) -> None:
    batch = await ReplayProvider(tmp_path, FrozenClock(NOW)).flights(DUBLIN, MORNING)

    assert len(batch) == 0
