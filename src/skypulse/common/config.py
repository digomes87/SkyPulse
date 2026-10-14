"""Runtime configuration.

One switch, ``SKYPULSE_RUN_MODE``, selects between the local docker-compose stack
(Kafka + LocalStack) and real AWS. Nested values use ``__`` as delimiter, for example
``SKYPULSE_KAFKA__BOOTSTRAP_SERVERS``.
"""

from enum import StrEnum
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
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

    def bucket_for(self, layer: LakeLayer) -> str:
        return f"{self.bucket_prefix}-{layer}"


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
