"""HTTP calls with bounded retries, exponential backoff with full jitter and Retry-After.

Only transient failures are retried: transport errors, 429 and 5xx. Every other response is
returned to the caller, which knows what a 404 or 401 means for its API.
"""

import asyncio
import random
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass

import httpx

from skypulse.common.logging import get_logger
from skypulse.observability.metrics import HTTP_RETRIES

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
RETRY_AFTER_HEADERS = ("Retry-After", "X-Rate-Limit-Retry-After-Seconds")

Sleep = Callable[[float], Awaitable[None]]
QueryParams = Mapping[str, str | int | float]

_log = get_logger(__name__)


class RetriesExhaustedError(RuntimeError):
    pass


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

    def backoff(self, attempt: int, jitter: float) -> float:
        exponential: float = self.base_delay_seconds * 2.0 ** (attempt - 1)
        ceiling = min(self.max_delay_seconds, exponential)
        return ceiling * jitter

    def capped(self, seconds: float) -> float:
        return min(seconds, self.max_delay_seconds)


def _retry_after(response: httpx.Response) -> float | None:
    for header in RETRY_AFTER_HEADERS:
        value = response.headers.get(header)
        if value is not None and value.replace(".", "", 1).isdigit():
            return float(value)
    return None


class RetryingHttp:
    def __init__(
        self,
        client: httpx.AsyncClient,
        policy: RetryPolicy,
        sleep: Sleep = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        self._client = client
        self._policy = policy
        self._sleep = sleep
        self._jitter = jitter

    async def get(
        self,
        url: str,
        params: QueryParams | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        last_failure = "no attempt made"
        for attempt in range(1, self._policy.max_attempts + 1):
            outcome = await self._attempt(url, params, headers)
            if isinstance(outcome, httpx.Response) and outcome.status_code not in RETRYABLE_STATUS:
                return outcome
            last_failure = self._describe(outcome)
            await self._wait(attempt, outcome, url, last_failure)
        raise RetriesExhaustedError(
            f"GET {url} failed after {self._policy.max_attempts} attempts: {last_failure}"
        )

    async def _attempt(
        self,
        url: str,
        params: QueryParams | None,
        headers: Mapping[str, str] | None,
    ) -> httpx.Response | httpx.TransportError:
        try:
            return await self._client.get(url, params=params, headers=headers)
        except httpx.TransportError as error:
            return error

    async def _wait(
        self, attempt: int, outcome: httpx.Response | Exception, url: str, failure: str
    ) -> None:
        if attempt == self._policy.max_attempts:
            return
        delay = self._delay(attempt, outcome)
        _log.warning("http_retry", url=url, attempt=attempt, delay_seconds=delay, reason=failure)
        HTTP_RETRIES.labels(httpx.URL(url).host).inc()
        await self._sleep(delay)

    def _delay(self, attempt: int, outcome: httpx.Response | Exception) -> float:
        hinted = _retry_after(outcome) if isinstance(outcome, httpx.Response) else None
        if hinted is not None:
            return self._policy.capped(hinted)
        return self._policy.backoff(attempt, self._jitter())

    @staticmethod
    def _describe(outcome: httpx.Response | Exception) -> str:
        if isinstance(outcome, httpx.Response):
            return f"HTTP {outcome.status_code}"
        return type(outcome).__name__
