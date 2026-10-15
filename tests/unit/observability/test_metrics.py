import httpx
import pytest
from prometheus_client import REGISTRY

from skypulse.common.config import Settings
from skypulse.common.http import RetryingHttp, RetryPolicy
from skypulse.ingestion.opensky.access import CreditTracker
from skypulse.ingestion.publisher import PublishReport
from skypulse.ingestion.runner import run_cycle
from skypulse.observability.metrics import start_metrics_server


def sample(name: str, labels: dict[str, str] | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


class OnePoll:
    def __init__(self, outcome: PublishReport | Exception) -> None:
        self.outcome = outcome

    async def run_once(self) -> PublishReport:
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


async def test_cycles_count_published_rejected_and_failures() -> None:
    feed = {"feed": "metrics-test"}
    before = (
        sample("skypulse_events_published_total", feed),
        sample("skypulse_events_rejected_total", feed),
        sample("skypulse_poll_failures_total", feed),
    )

    await run_cycle("metrics-test", OnePoll(PublishReport(5, 2)))
    await run_cycle("metrics-test", OnePoll(RuntimeError("boom")))

    assert sample("skypulse_events_published_total", feed) - before[0] == 5
    assert sample("skypulse_events_rejected_total", feed) - before[1] == 2
    assert sample("skypulse_poll_failures_total", feed) - before[2] == 1
    assert sample("skypulse_poll_duration_seconds_count", feed) >= 2


def test_credit_balance_is_exported() -> None:
    CreditTracker().observe(httpx.Response(200, headers={"X-Rate-Limit-Remaining": "321"}))

    assert sample("skypulse_opensky_credits_remaining") == 321


async def test_retries_are_counted_per_host() -> None:
    responses = iter([httpx.Response(503), httpx.Response(200)])
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: next(responses)))

    async def no_sleep(_: float) -> None:
        return None

    before = sample("skypulse_http_retries_total", {"host": "retry.example.test"})
    await RetryingHttp(client, RetryPolicy(max_attempts=2), no_sleep).get(
        "https://retry.example.test/x"
    )

    assert sample("skypulse_http_retries_total", {"host": "retry.example.test"}) - before == 1


def test_metrics_server_only_starts_when_a_port_is_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started: list[int] = []
    monkeypatch.setattr("skypulse.observability.metrics.start_http_server", started.append)

    assert start_metrics_server(Settings()) is False
    monkeypatch.setenv("SKYPULSE_METRICS__PORT", "9464")
    assert start_metrics_server(Settings()) is True
    assert started == [9464]
