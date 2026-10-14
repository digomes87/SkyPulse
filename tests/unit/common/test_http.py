import httpx
import pytest

from skypulse.common.http import RetriesExhaustedError, RetryingHttp, RetryPolicy

URL = "https://api.example.test/data"


class RecordingSleep:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


def scripted(*outcomes: httpx.Response | Exception) -> httpx.MockTransport:
    remaining = list(outcomes)

    def handler(request: httpx.Request) -> httpx.Response:
        outcome = remaining.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return httpx.MockTransport(handler)


def retrying(transport: httpx.MockTransport, sleep: RecordingSleep) -> RetryingHttp:
    policy = RetryPolicy(max_attempts=3, base_delay_seconds=1.0, max_delay_seconds=30.0)
    return RetryingHttp(httpx.AsyncClient(transport=transport), policy, sleep, jitter=lambda: 1.0)


async def test_returns_first_successful_response_without_sleeping() -> None:
    sleep = RecordingSleep()
    http = retrying(scripted(httpx.Response(200, json={"ok": True})), sleep)

    response = await http.get(URL)

    assert response.json() == {"ok": True}
    assert sleep.delays == []


async def test_retries_server_errors_with_exponential_backoff() -> None:
    sleep = RecordingSleep()
    transport = scripted(httpx.Response(503), httpx.Response(502), httpx.Response(200))

    response = await retrying(transport, sleep).get(URL)

    assert response.status_code == 200
    assert sleep.delays == [1.0, 2.0]


async def test_honours_retry_after_headers() -> None:
    sleep = RecordingSleep()
    limited = httpx.Response(429, headers={"X-Rate-Limit-Retry-After-Seconds": "7"})
    transport = scripted(limited, httpx.Response(200))

    await retrying(transport, sleep).get(URL)

    assert sleep.delays == [7.0]


async def test_caps_retry_after_at_the_policy_maximum() -> None:
    sleep = RecordingSleep()
    transport = scripted(httpx.Response(429, headers={"Retry-After": "3600"}), httpx.Response(200))

    await retrying(transport, sleep).get(URL)

    assert sleep.delays == [30.0]


async def test_retries_transport_errors() -> None:
    sleep = RecordingSleep()
    transport = scripted(httpx.ConnectError("refused"), httpx.Response(200))

    response = await retrying(transport, sleep).get(URL)

    assert response.status_code == 200


async def test_does_not_retry_client_errors() -> None:
    sleep = RecordingSleep()

    response = await retrying(scripted(httpx.Response(404)), sleep).get(URL)

    assert response.status_code == 404
    assert sleep.delays == []


async def test_gives_up_after_max_attempts() -> None:
    sleep = RecordingSleep()
    transport = scripted(httpx.Response(500), httpx.Response(500), httpx.Response(500))

    with pytest.raises(RetriesExhaustedError, match="3 attempts"):
        await retrying(transport, sleep).get(URL)


async def test_gives_up_after_repeated_transport_errors() -> None:
    sleep = RecordingSleep()
    failures = [httpx.ReadTimeout("slow") for _ in range(3)]

    with pytest.raises(RetriesExhaustedError, match="ReadTimeout"):
        await retrying(scripted(*failures), sleep).get(URL)


def test_policy_rejects_nonsensical_values() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=0)
