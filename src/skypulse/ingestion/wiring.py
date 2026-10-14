"""Builds pollers from settings. The only place that knows about concrete clients."""

from collections.abc import Callable
from enum import StrEnum

import httpx

from skypulse.common.clock import Clock, ReplayClock, SystemClock
from skypulse.common.config import RunMode, Settings
from skypulse.common.http import RetryingHttp, RetryPolicy
from skypulse.common.kafka import KafkaStreamSink
from skypulse.common.streams import StreamSink
from skypulse.contracts import EventSource
from skypulse.ingestion.airports import MonitoredAirports
from skypulse.ingestion.metar.client import AviationWeatherClient
from skypulse.ingestion.opensky.access import (
    Access,
    AnonymousAccess,
    ClientCredentialsAccess,
    CreditTracker,
)
from skypulse.ingestion.opensky.client import OpenSkyClient
from skypulse.ingestion.opensky.models import EUROPE
from skypulse.ingestion.pollers import (
    AircraftStatePoller,
    MetarPoller,
    PollContext,
    Poller,
    SchedulePoller,
)
from skypulse.ingestion.publisher import DEFAULT_ROUTES, EventPublisher
from skypulse.ingestion.recorded_transport import recorded_transport
from skypulse.ingestion.schedules.aerodatabox import AeroDataBoxProvider
from skypulse.ingestion.schedules.provider import ScheduleProvider
from skypulse.ingestion.schedules.replay import ReplayProvider

HTTP_TIMEOUT_SECONDS = 30.0


class Feed(StrEnum):
    STATES = "states"
    SCHEDULES = "schedules"
    METAR = "metar"

    def interval(self, settings: Settings) -> float:
        intervals = {
            Feed.STATES: settings.ingestion.states_interval_seconds,
            Feed.SCHEDULES: settings.ingestion.schedules_interval_seconds,
            Feed.METAR: settings.ingestion.metar_interval_seconds,
        }
        return intervals[self]


class MissingCredentialsError(RuntimeError):
    pass


class Wiring:
    def __init__(self, settings: Settings, sink: StreamSink) -> None:
        self._settings = settings
        self._sink = sink
        self._clock = self._build_clock()

    @classmethod
    def from_settings(cls, settings: Settings) -> "Wiring":
        if settings.run_mode is RunMode.AWS:
            raise NotImplementedError("the Kinesis sink for aws mode arrives in M8")
        return cls(settings, KafkaStreamSink.connect(settings.kafka.bootstrap_servers))

    def poller(self, feed: Feed) -> Poller:
        builders: dict[Feed, Callable[[], Poller]] = {
            Feed.STATES: self._states,
            Feed.SCHEDULES: self._schedules,
            Feed.METAR: self._metar,
        }
        return builders[feed]()

    def _states(self) -> Poller:
        client = OpenSkyClient(self._http(), self._opensky_access(), CreditTracker())
        return AircraftStatePoller(client, EUROPE, self._context(EventSource.OPENSKY))

    def _schedules(self) -> Poller:
        airports = MonitoredAirports.load()
        return SchedulePoller(
            self._schedule_provider(), airports, self._context(EventSource.AERODATABOX)
        )

    def _metar(self) -> Poller:
        client = AviationWeatherClient(self._http(), self._settings.aviationweather.user_agent)
        return MetarPoller(
            client, MonitoredAirports.load(), self._context(EventSource.AVIATIONWEATHER)
        )

    def _schedule_provider(self) -> ScheduleProvider:
        if self._settings.ingestion.replay:
            return ReplayProvider(
                self._settings.ingestion.fixtures_dir / "aerodatabox", self._clock
            )
        api_key = self._settings.aerodatabox.api_key
        if api_key is None:
            raise MissingCredentialsError("set SKYPULSE_AERODATABOX__API_KEY or enable replay")
        return AeroDataBoxProvider(self._http(), api_key, self._clock)

    def _opensky_access(self) -> Access:
        opensky = self._settings.opensky
        if (
            self._settings.ingestion.replay
            or opensky.client_id is None
            or opensky.client_secret is None
        ):
            return AnonymousAccess()
        token_http = httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS)
        return ClientCredentialsAccess(
            opensky.client_id, opensky.client_secret, token_http, self._clock
        )

    def _http(self) -> RetryingHttp:
        transport = None
        if self._settings.ingestion.replay:
            transport = recorded_transport(self._settings.ingestion.fixtures_dir, self._clock)
        client = httpx.AsyncClient(transport=transport, timeout=HTTP_TIMEOUT_SECONDS)
        return RetryingHttp(client, RetryPolicy())

    def _context(self, live_source: EventSource) -> PollContext:
        source = EventSource.REPLAY if self._settings.ingestion.replay else live_source
        return PollContext(EventPublisher(self._sink, DEFAULT_ROUTES), self._clock, source)

    def _build_clock(self) -> Clock:
        ingestion = self._settings.ingestion
        if ingestion.replay:
            return ReplayClock(ingestion.replay_start, ingestion.replay_speed)
        return SystemClock()
