"""AeroDataBox (RapidAPI) flight information display endpoint."""

from datetime import datetime, timedelta

import httpx
from pydantic import SecretStr

from skypulse.common.aviation import TimeWindow
from skypulse.common.clock import Clock
from skypulse.common.http import RetryingHttp
from skypulse.ingestion.airports import MonitoredAirport
from skypulse.ingestion.schedules.models import ScheduleBatch

RAPIDAPI_HOST = "aerodatabox.p.rapidapi.com"
BASE_URL = f"https://{RAPIDAPI_HOST}"
# The FIDS endpoint caps one request at 12 hours of local time.
MAXIMUM_WINDOW = timedelta(hours=12)
QUERY = {
    "withLeg": "true",
    "direction": "Both",
    "withCancelled": "true",
    "withCodeshared": "false",
    "withCargo": "false",
    "withPrivate": "false",
}


class AeroDataBoxProvider:
    def __init__(self, http: RetryingHttp, api_key: SecretStr, clock: Clock) -> None:
        self._http = http
        self._headers = {
            "X-RapidAPI-Key": api_key.get_secret_value(),
            "X-RapidAPI-Host": RAPIDAPI_HOST,
        }
        self._clock = clock

    async def flights(self, airport: MonitoredAirport, window: TimeWindow) -> ScheduleBatch:
        batch = ScheduleBatch(())
        for chunk in window.split(MAXIMUM_WINDOW):
            batch = batch.merged_with(await self._fetch(airport, chunk))
        return batch

    async def _fetch(self, airport: MonitoredAirport, chunk: TimeWindow) -> ScheduleBatch:
        start = self._local(chunk.start, airport)
        end = self._local(chunk.end, airport)
        url = f"{BASE_URL}/flights/airports/icao/{airport.icao}/{start}/{end}"
        response = await self._http.get(url, params=QUERY, headers=self._headers)
        if response.status_code == httpx.codes.NO_CONTENT:
            return ScheduleBatch(())
        response.raise_for_status()
        return ScheduleBatch.from_fids(response.json(), airport.icao, self._clock.now())

    @staticmethod
    def _local(instant: datetime, airport: MonitoredAirport) -> str:
        return instant.astimezone(airport.timezone).strftime("%Y-%m-%dT%H:%M")
