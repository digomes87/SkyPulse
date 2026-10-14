from datetime import UTC, datetime, timedelta

import httpx
import pytest

from skypulse.common.aviation import AirportIcao, TimeWindow
from skypulse.common.http import RetryingHttp, RetryPolicy
from skypulse.ingestion.opensky.access import AnonymousAccess, CreditsExhaustedError, CreditTracker
from skypulse.ingestion.opensky.client import BASE_URL, OpenSkyClient
from skypulse.ingestion.opensky.models import BoundingBox, Coordinate

IRELAND = BoundingBox(Coordinate(51, -11), Coordinate(56, -5))
ROW = ["4ca7b5", "RYR8TK  ", "Ireland", 1, 2, -7.6, 52.3, 100.0, False, 200.0, 280.0, 0,
       None, 110.0, "2120", False, 0]  # fmt: skip
DAY = datetime(2026, 10, 13, tzinfo=UTC)


class RefreshingAccess(AnonymousAccess):
    def __init__(self) -> None:
        self.invalidations = 0

    async def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer t{self.invalidations}"}

    def invalidate(self) -> None:
        self.invalidations += 1


def client_for(
    handler: httpx.MockTransport, access: AnonymousAccess | None = None
) -> tuple[OpenSkyClient, CreditTracker]:
    async def no_sleep(_: float) -> None:
        return None

    http = RetryingHttp(httpx.AsyncClient(transport=handler), RetryPolicy(max_attempts=2), no_sleep)
    credits = CreditTracker()
    return OpenSkyClient(http, access or AnonymousAccess(), credits), credits


async def test_states_returns_a_snapshot_and_tracks_credits() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        body = {"time": 1791972200, "states": [ROW, ROW]}
        return httpx.Response(200, json=body, headers={"X-Rate-Limit-Remaining": "398"})

    client, credits = client_for(httpx.MockTransport(handler))

    snapshot = await client.states(IRELAND)

    assert len(snapshot) == 2
    assert next(iter(snapshot)).data["snapshot_time"] == 1791972200
    assert seen[0].url.path == "/api/states/all"
    assert seen[0].url.params["lamin"] == "51"
    assert credits.remaining() == 398


async def test_states_with_no_aircraft_is_an_empty_snapshot() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"time": 1, "states": None})

    client, _ = client_for(httpx.MockTransport(handler))

    assert len(await client.states(IRELAND)) == 0


async def test_states_refuses_to_spend_credits_it_does_not_have() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"time": 1, "states": []}, headers={"X-Rate-Limit-Remaining": "0"}
        )

    client, _ = client_for(httpx.MockTransport(handler))
    await client.states(IRELAND)

    with pytest.raises(CreditsExhaustedError):
        await client.states(IRELAND)


async def test_expired_token_is_refreshed_once() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.headers["Authorization"] == "Bearer t0":
            return httpx.Response(401)
        return httpx.Response(200, json={"time": 1, "states": []})

    access = RefreshingAccess()
    client, _ = client_for(httpx.MockTransport(handler), access)

    await client.states(IRELAND)

    assert access.invalidations == 1


async def test_departures_parse_flights_and_treat_404_as_empty() -> None:
    flight = {
        "icao24": "4ca7b5",
        "firstSeen": 1791900000,
        "lastSeen": 1791906000,
        "estDepartureAirport": "EIDW",
        "estArrivalAirport": "LEMD",
        "callsign": "RYR7241",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["begin"] == str(int(DAY.timestamp())):
            return httpx.Response(200, json=[flight])
        return httpx.Response(404)

    client, _ = client_for(httpx.MockTransport(handler))
    window = TimeWindow(DAY, DAY + timedelta(days=3))

    flights = await client.departures(AirportIcao("EIDW"), window)

    assert [item.data["callsign"] for item in flights] == ["RYR7241"]


async def test_arrivals_are_split_into_two_day_requests() -> None:
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request.url.path)
        return httpx.Response(404)

    client, _ = client_for(httpx.MockTransport(handler))

    await client.arrivals(AirportIcao("LEMD"), TimeWindow(DAY, DAY + timedelta(days=5)))

    assert requested == ["/api/flights/arrival"] * 3


def test_base_url_points_at_the_public_api() -> None:
    assert BASE_URL == "https://opensky-network.org/api"
