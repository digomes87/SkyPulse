"""Authentication and credit accounting for the OpenSky REST API.

OpenSky accepts OAuth2 client credentials only (basic auth was retired). Tokens live for
30 minutes; they are refreshed 30 seconds early so a request never races the expiry.
Every endpoint family has a daily credit budget, reported in X-Rate-Limit-Remaining.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

import httpx
from pydantic import SecretStr

from skypulse.common.clock import Clock
from skypulse.observability.metrics import OPENSKY_CREDITS

# An endpoint, not a credential.
TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"  # noqa: S105
)
REFRESH_MARGIN = timedelta(seconds=30)
REMAINING_HEADER = "X-Rate-Limit-Remaining"


class CreditsExhaustedError(RuntimeError):
    pass


class Access(Protocol):
    async def headers(self) -> Mapping[str, str]: ...

    def invalidate(self) -> None: ...


class AnonymousAccess:
    async def headers(self) -> Mapping[str, str]:
        return {}

    def invalidate(self) -> None:
        return None


@dataclass(frozen=True)
class AccessToken:
    value: str
    expires_at: datetime

    def is_fresh(self, now: datetime) -> bool:
        return now < self.expires_at - REFRESH_MARGIN


class ClientCredentialsAccess:
    def __init__(
        self,
        client_id: str,
        client_secret: SecretStr,
        http: httpx.AsyncClient,
        clock: Clock,
    ) -> None:
        self._form = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret.get_secret_value(),
        }
        self._http = http
        self._clock = clock
        self._token: AccessToken | None = None

    async def headers(self) -> Mapping[str, str]:
        token = await self._valid_token()
        return {"Authorization": f"Bearer {token.value}"}

    def invalidate(self) -> None:
        self._token = None

    async def _valid_token(self) -> AccessToken:
        now = self._clock.now()
        if self._token is not None and self._token.is_fresh(now):
            return self._token
        self._token = await self._request_token(now)
        return self._token

    async def _request_token(self, now: datetime) -> AccessToken:
        response = await self._http.post(TOKEN_URL, data=self._form)
        response.raise_for_status()
        body = response.json()
        return AccessToken(body["access_token"], now + timedelta(seconds=body["expires_in"]))


class CreditTracker:
    """Keeps the last balance OpenSky reported and refuses calls that cannot be afforded."""

    def __init__(self) -> None:
        self._remaining: int | None = None

    def observe(self, response: httpx.Response) -> None:
        value = response.headers.get(REMAINING_HEADER)
        if value is None or not value.isdigit():
            return
        self._remaining = int(value)
        OPENSKY_CREDITS.set(self._remaining)

    def ensure_affordable(self, cost: int) -> None:
        if self._remaining is None or self._remaining >= cost:
            return
        raise CreditsExhaustedError(
            f"request needs {cost} credits, only {self._remaining} left today"
        )

    def remaining(self) -> int | None:
        return self._remaining
