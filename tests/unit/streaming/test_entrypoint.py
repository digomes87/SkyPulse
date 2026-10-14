import pytest

from skypulse.common.config import Settings
from skypulse.streaming.__main__ import Stage, parse_arguments


def test_bronze_needs_kafka_and_silver_does_not() -> None:
    settings = Settings()

    assert Stage.BRONZE.connectors(settings).kafka
    assert not Stage.SILVER.connectors(settings).kafka


def test_s3a_is_only_loaded_when_the_lake_lives_in_buckets(monkeypatch: pytest.MonkeyPatch) -> None:
    assert Stage.SILVER.connectors(Settings()).s3a

    monkeypatch.setenv("SKYPULSE_LAKE__ROOT_URI", "file:///tmp/lake")

    assert not Stage.SILVER.connectors(Settings()).s3a


def test_cli_arguments() -> None:
    arguments = parse_arguments(["silver", "--available-now", "--master", "local[2]"])

    assert arguments.stage is Stage.SILVER
    assert arguments.available_now
    assert arguments.master == "local[2]"
