from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import SecretStr

from skypulse.ingestion.opensky.access import (
    TOKEN_URL,
    AnonymousAccess,
    ClientCredentialsAccess,
    CreditsExhaustedError,
    CreditTracker,
)

NOW = datetime(2026, 10, 14, 10, 0, tzinfo=UTC)


class MovableClock:
    def __init__(self) -> None:
        self.instant = NOW

    def now(self) -> datetime:
        return self.instant


def token_server(issued: list[httpx.Request]) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        issued.append(request)
        return httpx.Response(
            200, json={"access_token": f"token-{len(issued)}", "expires_in": 1800}
        )

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def credentials(issued: list[httpx.Request], clock: MovableClock) -> ClientCredentialsAccess:
    return ClientCredentialsAccess(
        "skypulse-client", SecretStr("s3cret"), token_server(issued), clock
    )


async def test_anonymous_access_sends_no_authorization() -> None:
    assert await AnonymousAccess().headers() == {}


async def test_client_credentials_request_a_bearer_token() -> None:
    issued: list[httpx.Request] = []

    headers = await credentials(issued, MovableClock()).headers()

    assert headers == {"Authorization": "Bearer token-1"}
    assert str(issued[0].url) == TOKEN_URL
    body = issued[0].content.decode()
    assert "grant_type=client_credentials" in body
    assert "client_id=skypulse-client" in body


async def test_token_is_reused_until_shortly_before_expiry() -> None:
    issued: list[httpx.Request] = []
    clock = MovableClock()
    access = credentials(issued, clock)

    await access.headers()
    clock.instant = NOW + timedelta(minutes=29)
    await access.headers()
    assert len(issued) == 1

    clock.instant = NOW + timedelta(minutes=29, seconds=31)
    assert await access.headers() == {"Authorization": "Bearer token-2"}


async def test_invalidate_forces_a_new_token() -> None:
    issued: list[httpx.Request] = []
    access = credentials(issued, MovableClock())

    await access.headers()
    access.invalidate()
    await access.headers()

    assert len(issued) == 2


async def test_token_endpoint_failure_is_reported() -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(401)))
    access = ClientCredentialsAccess("id", SecretStr("wrong"), client, MovableClock())

    with pytest.raises(httpx.HTTPStatusError):
        await access.headers()


def test_credit_tracker_allows_calls_while_balance_is_unknown() -> None:
    CreditTracker().ensure_affordable(4)


def test_credit_tracker_reads_the_remaining_balance() -> None:
    tracker = CreditTracker()

    tracker.observe(httpx.Response(200, headers={"X-Rate-Limit-Remaining": "3"}))

    assert tracker.remaining() == 3
    with pytest.raises(CreditsExhaustedError, match="needs 4"):
        tracker.ensure_affordable(4)


def test_credit_tracker_ignores_responses_without_the_header() -> None:
    tracker = CreditTracker()

    tracker.observe(httpx.Response(200, headers={"X-Rate-Limit-Remaining": "10"}))
    tracker.observe(httpx.Response(200))

    assert tracker.remaining() == 10
