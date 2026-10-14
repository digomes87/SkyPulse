"""Client for https://aviationweather.gov/api/data/metar.

The API allows 100 requests a minute and returns at most 400 entries per call, so stations
are requested in batches. A descriptive User-Agent is asked for by the operators.
"""

from collections.abc import Sequence
from itertools import batched

import httpx

from skypulse.common.aviation import AirportIcao
from skypulse.common.http import QueryParams, RetryingHttp
from skypulse.ingestion.metar.models import MetarBatch

METAR_URL = "https://aviationweather.gov/api/data/metar"
DEFAULT_BATCH_SIZE = 20


class AviationWeatherClient:
    def __init__(
        self, http: RetryingHttp, user_agent: str, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> None:
        self._http = http
        self._headers = {"User-Agent": user_agent}
        self._batch_size = batch_size

    async def observations(self, stations: Sequence[AirportIcao], hours: int) -> MetarBatch:
        batch = MetarBatch(())
        for group in batched(stations, self._batch_size):
            batch = batch.merged_with(await self._fetch(group, hours))
        return batch

    async def _fetch(self, stations: Sequence[AirportIcao], hours: int) -> MetarBatch:
        query: QueryParams = {
            "ids": ",".join(str(code) for code in stations),
            "format": "json",
            "hours": hours,
        }
        response = await self._http.get(METAR_URL, params=query, headers=self._headers)
        if response.status_code == httpx.codes.NO_CONTENT:
            return MetarBatch(())
        response.raise_for_status()
        return MetarBatch.from_payload(response.json())
