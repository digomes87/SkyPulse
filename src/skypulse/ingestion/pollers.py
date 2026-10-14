"""One polling cycle per source: fetch, turn into events, publish."""

from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

from skypulse.common.aviation import TimeWindow
from skypulse.common.clock import Clock
from skypulse.contracts import EventSource
from skypulse.ingestion.airports import MonitoredAirports
from skypulse.ingestion.metar.client import AviationWeatherClient
from skypulse.ingestion.opensky.client import OpenSkyClient
from skypulse.ingestion.opensky.models import BoundingBox
from skypulse.ingestion.publisher import EventPublisher, PublishReport
from skypulse.ingestion.schedules.provider import ScheduleProvider

SCHEDULE_LOOKBACK = timedelta(hours=3)
SCHEDULE_LOOKAHEAD = timedelta(hours=9)
METAR_HOURS = 2


class Poller(Protocol):
    async def run_once(self) -> PublishReport: ...


@dataclass(frozen=True)
class PollContext:
    publisher: EventPublisher
    clock: Clock
    source: EventSource


class AircraftStatePoller:
    def __init__(self, client: OpenSkyClient, box: BoundingBox, context: PollContext) -> None:
        self._client = client
        self._box = box
        self._context = context

    async def run_once(self) -> PublishReport:
        snapshot = await self._client.states(self._box)
        produced_at = self._context.clock.now()
        events = [state.to_event(self._context.source, produced_at) for state in snapshot]
        return await self._context.publisher.publish(events)


class SchedulePoller:
    def __init__(
        self, provider: ScheduleProvider, airports: MonitoredAirports, context: PollContext
    ) -> None:
        self._provider = provider
        self._airports = airports
        self._context = context

    async def run_once(self) -> PublishReport:
        now = self._context.clock.now()
        window = TimeWindow(now - SCHEDULE_LOOKBACK, now + SCHEDULE_LOOKAHEAD)
        report = PublishReport(0, 0)
        for airport in self._airports:
            batch = await self._provider.flights(airport, window)
            events = [flight.to_event(self._context.source) for flight in batch]
            report = report.plus(await self._context.publisher.publish(events))
        return report


class MetarPoller:
    def __init__(
        self, client: AviationWeatherClient, airports: MonitoredAirports, context: PollContext
    ) -> None:
        self._client = client
        self._airports = airports
        self._context = context

    async def run_once(self) -> PublishReport:
        batch = await self._client.observations(self._airports.codes(), METAR_HOURS)
        produced_at = self._context.clock.now()
        events = [observation.to_event(self._context.source, produced_at) for observation in batch]
        return await self._context.publisher.publish(events)
