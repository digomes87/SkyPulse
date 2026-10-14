"""Replay a whole day, detect disruptions and check the EU261 claims in DynamoDB.

Producers -> Kafka -> bronze -> silver -> detector -> SNS -> SQS -> Lambda -> DynamoDB,
all on the local stack. Lambdas run in LocalStack containers.
"""

import asyncio
import time
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
import pytest

from skypulse.common.config import Settings
from skypulse.deployment.lambdas import main as deploy_lambdas
from skypulse.ingestion.__main__ import main as ingest
from skypulse.streaming.__main__ import main as stream

pytestmark = [pytest.mark.integration, pytest.mark.spark]

FIXTURES = Path(__file__).parents[2] / "data" / "fixtures"
CLAIM_WAIT_SECONDS = 180
POLL_SECONDS = 3


@pytest.fixture(scope="module")
def settings(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Settings]:
    patch = pytest.MonkeyPatch()
    patch.setenv("SKYPULSE_INGESTION__REPLAY", "true")
    patch.setenv("SKYPULSE_INGESTION__FIXTURES_DIR", str(FIXTURES))
    patch.setenv("SKYPULSE_LAKE__ROOT_URI", tmp_path_factory.mktemp("lake").as_uri())
    patch.setenv("AWS_ACCESS_KEY_ID", "test")
    patch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    yield Settings()
    patch.undo()


def replay(feed: str, start: str, cycles: int = 1, speed: str = "60") -> None:
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("SKYPULSE_INGESTION__REPLAY_START", start)
        patch.setenv("SKYPULSE_INGESTION__REPLAY_SPEED", speed)
        assert asyncio.run(ingest([feed, "--cycles", str(cycles), "--interval", "1"])) == 0


@pytest.fixture(scope="module")
def claims(settings: Settings) -> dict[str, dict[str, Any]]:
    # 1 s of wall time is 2 h of replay time: 10 schedule polls cover 05:00-23:00Z.
    replay("schedules", "2026-10-14T05:00:00Z", cycles=10, speed="7200")
    replay("metar", "2026-10-14T23:00:00Z")
    for stage in ("bronze", "silver"):
        assert stream([stage, "--available-now", "--master", "local[2]"]) == 0
    deploy_lambdas()
    assert stream(["detector", "--available-now", "--master", "local[2]"]) == 0
    return wait_for_claims(settings, {"EI105", "FR1234", "FR6403"})


def wait_for_claims(settings: Settings, flights: set[str]) -> dict[str, dict[str, Any]]:
    table = boto3.resource(
        "dynamodb", endpoint_url=settings.aws_endpoint_url, region_name=settings.aws.region
    ).Table("eu261-claims")
    deadline = time.monotonic() + CLAIM_WAIT_SECONDS
    found: dict[str, dict[str, Any]] = {}
    while time.monotonic() < deadline and not flights <= found.keys():
        found = {item["flight_number"]: item for item in table.scan()["Items"]}
        time.sleep(POLL_SECONDS)
    return found


def test_long_haul_delay_claims_600(claims: dict[str, dict[str, Any]]) -> None:
    assert claims["EI105"]["verdict"] == "eligible"
    assert claims["EI105"]["amount_eur"] == Decimal("600")


def test_same_day_cancellation_is_eligible(claims: dict[str, dict[str, Any]]) -> None:
    assert claims["FR1234"]["kind"] == "cancellation"
    assert claims["FR1234"]["amount_eur"] == Decimal("250")


def test_thunderstorm_delay_is_flagged_as_likely_exempt(claims: dict[str, dict[str, Any]]) -> None:
    claim = claims["FR6403"]

    assert claim["verdict"] == "eligible"
    assert claim["extraordinary_circumstances_likely"] is True
    assert "thunderstorm" in claim["severe_weather"]


def test_claims_carry_the_correlation_id_from_ingestion(claims: dict[str, dict[str, Any]]) -> None:
    assert all(claim["correlation_id"] for claim in claims.values())
