"""Async client for the OpenSky Network REST API."""

from datetime import timedelta

import httpx

from skypulse.common.aviation import AirportIcao, TimeWindow
from skypulse.common.http import QueryParams, RetryingHttp
from skypulse.ingestion.opensky.access import Access, CreditTracker
from skypulse.ingestion.opensky.models import BoundingBox, ObservedFlight, StateSnapshot

BASE_URL = "https://opensky-network.org/api"
# /flights endpoints reject intervals longer than two days.
MAXIMUM_FLIGHTS_INTERVAL = timedelta(days=2)
FLIGHTS_CREDIT_COST = 4


class OpenSkyClient:
    def __init__(self, http: RetryingHttp, access: Access, credits: CreditTracker) -> None:
        self._http = http
        self._access = access
        self._credits = credits

    async def states(self, box: BoundingBox) -> StateSnapshot:
        response = await self._get("/states/all", box.as_query(), box.credit_cost())
        response.raise_for_status()
        return StateSnapshot.from_payload(response.json())

    async def departures(self, airport: AirportIcao, window: TimeWindow) -> list[ObservedFlight]:
        return await self._flights("/flights/departure", airport, window)

    async def arrivals(self, airport: AirportIcao, window: TimeWindow) -> list[ObservedFlight]:
        return await self._flights("/flights/arrival", airport, window)

    async def _flights(
        self, path: str, airport: AirportIcao, window: TimeWindow
    ) -> list[ObservedFlight]:
        flights: list[ObservedFlight] = []
        for chunk in window.split(MAXIMUM_FLIGHTS_INTERVAL):
            flights.extend(await self._flights_chunk(path, airport, chunk))
        return flights

    async def _flights_chunk(
        self, path: str, airport: AirportIcao, chunk: TimeWindow
    ) -> list[ObservedFlight]:
        query: QueryParams = {
            "airport": str(airport),
            "begin": int(chunk.start.timestamp()),
            "end": int(chunk.end.timestamp()),
        }
        response = await self._get(path, query, FLIGHTS_CREDIT_COST)
        if response.status_code == httpx.codes.NOT_FOUND:
            return []
        response.raise_for_status()
        return [ObservedFlight.from_payload(item) for item in response.json()]

    async def _get(self, path: str, query: QueryParams, cost: int) -> httpx.Response:
        self._credits.ensure_affordable(cost)
        response = await self._authorised_get(path, query)
        if response.status_code == httpx.codes.UNAUTHORIZED:
            self._access.invalidate()
            response = await self._authorised_get(path, query)
        self._credits.observe(response)
        return response

    async def _authorised_get(self, path: str, query: QueryParams) -> httpx.Response:
        headers = await self._access.headers()
        return await self._http.get(f"{BASE_URL}{path}", params=query, headers=headers)
