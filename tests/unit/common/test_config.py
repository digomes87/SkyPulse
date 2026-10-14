import pytest
from pydantic import ValidationError

from skypulse.common.config import LakeLayer, RunMode, Settings


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir("/")
    for name in ("SKYPULSE_RUN_MODE", "SKYPULSE_AWS__ENDPOINT_URL", "SKYPULSE_LOG_LEVEL"):
        monkeypatch.delenv(name, raising=False)


def test_defaults_to_local_mode_against_localstack() -> None:
    settings = Settings()

    assert settings.run_mode is RunMode.LOCAL
    assert settings.aws_endpoint_url == "http://localhost:4566"
    assert settings.kafka.bootstrap_servers == "localhost:9092"


def test_reads_nested_values_from_prefixed_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SKYPULSE_KAFKA__BOOTSTRAP_SERVERS", "kafka:19092")
    monkeypatch.setenv("SKYPULSE_LOG_LEVEL", "DEBUG")

    settings = Settings()

    assert settings.kafka.bootstrap_servers == "kafka:19092"
    assert settings.log_level == "DEBUG"


def test_aws_mode_drops_the_local_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SKYPULSE_RUN_MODE", "aws")

    settings = Settings()

    assert settings.run_mode is RunMode.AWS
    assert settings.aws_endpoint_url is None


def test_aws_mode_rejects_an_explicit_localstack_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SKYPULSE_RUN_MODE", "aws")
    monkeypatch.setenv("SKYPULSE_AWS__ENDPOINT_URL", "http://localhost:4566")

    with pytest.raises(ValidationError, match="endpoint"):
        Settings()


def test_local_mode_accepts_an_endpoint_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SKYPULSE_AWS__ENDPOINT_URL", "http://localstack:4566")

    assert Settings().aws_endpoint_url == "http://localstack:4566"


def test_rejects_unknown_run_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SKYPULSE_RUN_MODE", "staging")

    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize(
    ("layer", "expected"),
    [
        (LakeLayer.BRONZE, "skypulse-bronze"),
        (LakeLayer.SILVER, "skypulse-silver"),
        (LakeLayer.GOLD, "skypulse-gold"),
        (LakeLayer.CHECKPOINTS, "skypulse-checkpoints"),
    ],
)
def test_lake_bucket_name_per_layer(layer: LakeLayer, expected: str) -> None:
    assert Settings().lake.bucket_for(layer) == expected


def test_lake_bucket_prefix_must_be_a_valid_bucket_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SKYPULSE_LAKE__BUCKET_PREFIX", "Not_Valid")

    with pytest.raises(ValidationError):
        Settings()
