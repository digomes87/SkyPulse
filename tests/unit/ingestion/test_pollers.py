import json
from datetime import UTC, datetime
from pathlib import Path

import httpx

from skypulse.common.clock import FrozenClock
from skypulse.common.http import RetryingHttp, RetryPolicy
from skypulse.common.streams import InMemoryStream
from skypulse.contracts import EventSource, EventType
from skypulse.ingestion.airports import MonitoredAirports
from skypulse.ingestion.metar.client import AviationWeatherClient
from skypulse.ingestion.opensky.access import AnonymousAccess, CreditTracker
from skypulse.ingestion.opensky.client import OpenSkyClient
from skypulse.ingestion.opensky.models import EUROPE
from skypulse.ingestion.pollers import AircraftStatePoller, MetarPoller, PollContext, SchedulePoller
from skypulse.ingestion.publisher import DEFAULT_ROUTES, EventPublisher
from skypulse.ingestion.recorded_transport import recorded_transport
from skypulse.ingestion.schedules.replay import ReplayProvider
from tests.unit.ingestion.schedules.fids_sample import departure, fids

NOW = datetime(2026, 10, 14, 8, 0, tzinfo=UTC)
ROW = ["4ca7b5", "RYR8TK  ", "Ireland", 1, 2, -7.6, 52.3, 100.0, False, 200.0, 280.0, 0,
       None, 110.0, "2120", False, 0]  # fmt: skip


def write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def context(stream: InMemoryStream) -> PollContext:
    return PollContext(EventPublisher(stream, DEFAULT_ROUTES), FrozenClock(NOW), EventSource.REPLAY)


def http_over(root: Path) -> RetryingHttp:
    transport = recorded_transport(root, FrozenClock(NOW))
    return RetryingHttp(httpx.AsyncClient(transport=transport), RetryPolicy(max_attempts=1))


async def topic_size(stream: InMemoryStream, event_type: EventType) -> int:
    return len([item async for item in stream.consume(DEFAULT_ROUTES.topic_for(event_type))])


async def test_aircraft_state_poller_publishes_each_vector(tmp_path: Path) -> None:
    write(tmp_path / "opensky/states/20261014T0700Z.json", {"time": 1, "states": [ROW, ROW]})
    stream = InMemoryStream()
    client = OpenSkyClient(http_over(tmp_path), AnonymousAccess(), CreditTracker())

    report = await AircraftStatePoller(client, EUROPE, context(stream)).run_once()

    assert report.published == 2
    assert await topic_size(stream, EventType.AIRCRAFT_STATE) == 2


async def test_schedule_poller_covers_every_monitored_airport(tmp_path: Path) -> None:
    write(tmp_path / "EIDW/20261014T0600Z.json", fids(departure()))
    stream = InMemoryStream()
    poller = SchedulePoller(
        ReplayProvider(tmp_path, FrozenClock(NOW)), MonitoredAirports.load(), context(stream)
    )

    report = await poller.run_once()

    assert report.published == 1
    assert await topic_size(stream, EventType.FLIGHT_SCHEDULE) == 1


async def test_metar_poller_publishes_observations(tmp_path: Path) -> None:
    observation = {"icaoId": "EIDW", "obsTime": 1791970200, "metarType": "METAR",
                   "rawOb": "METAR EIDW 140930Z 19015KT 9999 15/10 Q1019"}  # fmt: skip
    write(tmp_path / "metar/20261014T0700Z.json", [observation])
    stream = InMemoryStream()
    client = AviationWeatherClient(http_over(tmp_path), "test-agent")

    report = await MetarPoller(client, MonitoredAirports.load(), context(stream)).run_once()

    assert report.published == 1
