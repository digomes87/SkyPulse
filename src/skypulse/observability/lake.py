"""When did each lake table last change? Read from the Delta log with delta-rs (no Spark)."""

from dataclasses import dataclass
from datetime import UTC, datetime

from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from skypulse.common.config import LakeLayer, Settings
from skypulse.streaming.tables import SILVER_TABLES

EXTRA_SILVER_TABLES = (
    "flight_disruptions",
    "eu261_assessments",
    "reference_airports",
    "quarantine",
)


@dataclass(frozen=True)
class LakeTable:
    layer: LakeLayer
    name: str

    def uri(self, settings: Settings) -> str:
        return f"{settings.lake.uri_for(self.layer)}/{self.name}".replace("s3a://", "s3://", 1)


LAKE_TABLES = (
    LakeTable(LakeLayer.BRONZE, "raw_events"),
    *(LakeTable(LakeLayer.SILVER, table.name) for table in SILVER_TABLES),
    *(LakeTable(LakeLayer.SILVER, name) for name in EXTRA_SILVER_TABLES),
)


def storage_options(settings: Settings) -> dict[str, str]:
    endpoint = settings.aws_endpoint_url
    options = {"AWS_REGION": settings.aws.region}
    if endpoint is not None:
        options |= {
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ALLOW_HTTP": str(endpoint.startswith("http://")).lower(),
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }
    return options


def last_commit(table: LakeTable, settings: Settings) -> datetime | None:
    try:
        delta = DeltaTable(table.uri(settings), storage_options=storage_options(settings))
    except TableNotFoundError:
        return None
    latest = delta.history(1)[0]
    return datetime.fromtimestamp(latest["timestamp"] / 1000, UTC)
