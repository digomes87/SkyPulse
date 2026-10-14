"""Runtime configuration.

One switch, ``SKYPULSE_RUN_MODE``, selects between the local docker-compose stack
(Kafka + LocalStack) and real AWS. Nested values use ``__`` as delimiter, for example
``SKYPULSE_KAFKA__BOOTSTRAP_SERVERS``.
"""

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LOCALSTACK_ENDPOINT = "http://localhost:4566"

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]


class RunMode(StrEnum):
    LOCAL = "local"
    AWS = "aws"


class LakeLayer(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    CHECKPOINTS = "checkpoints"


class KafkaSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    bootstrap_servers: str = "localhost:9092"


class AwsSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    region: str = "eu-west-1"
    endpoint_url: str | None = None


class LakeSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    bucket_prefix: str = Field(default="skypulse", pattern=r"^[a-z0-9][a-z0-9-]{1,40}[a-z0-9]$")
    root_uri: str | None = Field(
        default=None,
        description="Put every layer under one URI (e.g. file:///tmp/lake) instead of S3 buckets.",
    )

    def bucket_for(self, layer: LakeLayer) -> str:
        return f"{self.bucket_prefix}-{layer}"

    def uri_for(self, layer: LakeLayer) -> str:
        if self.root_uri is not None:
            return f"{self.root_uri.rstrip('/')}/{layer}"
        return f"s3a://{self.bucket_for(layer)}"


class OpenSkySettings(BaseModel):
    """Without credentials the client runs anonymously: /states/all only, 400 credits a day."""

    model_config = ConfigDict(frozen=True)

    client_id: str | None = None
    client_secret: SecretStr | None = None


class AeroDataBoxSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    api_key: SecretStr | None = None


class AviationWeatherSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    user_agent: str = "SkyPulse/0.1 (+https://github.com/digomes87/SkyPulse)"


class IngestionSettings(BaseModel):
    """Replay mode serves recorded fixtures through the real clients (see ADR 0004)."""

    model_config = ConfigDict(frozen=True)

    replay: bool = False
    fixtures_dir: Path = Path("data/fixtures")
    replay_start: datetime = datetime(2026, 10, 14, 5, 0, tzinfo=UTC)
    replay_speed: float = Field(default=60.0, gt=0)
    states_interval_seconds: float = Field(default=10.0, gt=0)
    schedules_interval_seconds: float = Field(default=300.0, gt=0)
    metar_interval_seconds: float = Field(default=600.0, gt=0)


class ReferenceSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    ourairports_base_url: str = "https://davidmegginson.github.io/ourairports-data"


class EventsSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    disruptions_topic: str = "flight-disruptions"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SKYPULSE_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
        frozen=True,
    )

    run_mode: RunMode = RunMode.LOCAL
    log_level: LogLevel = "INFO"
    kafka: KafkaSettings = KafkaSettings()
    aws: AwsSettings = AwsSettings()
    lake: LakeSettings = LakeSettings()
    opensky: OpenSkySettings = OpenSkySettings()
    aerodatabox: AeroDataBoxSettings = AeroDataBoxSettings()
    aviationweather: AviationWeatherSettings = AviationWeatherSettings()
    ingestion: IngestionSettings = IngestionSettings()
    events: EventsSettings = EventsSettings()
    reference: ReferenceSettings = ReferenceSettings()

    @model_validator(mode="after")
    def _forbid_endpoint_override_on_aws(self) -> Self:
        if self.run_mode is RunMode.AWS and self.aws.endpoint_url is not None:
            raise ValueError("aws.endpoint_url must not be set when run_mode is 'aws'")
        return self

    @property
    def aws_endpoint_url(self) -> str | None:
        """Endpoint for boto3 clients: LocalStack in local mode, the AWS default otherwise."""
        if self.run_mode is RunMode.AWS:
            return None
        return self.aws.endpoint_url or LOCALSTACK_ENDPOINT
