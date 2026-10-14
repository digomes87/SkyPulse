"""Smoke checks for the docker-compose stack. Run with `make up` then `make test-integration`."""

from pathlib import Path

import boto3
import pytest
from confluent_kafka.admin import AdminClient

from skypulse.common.config import LakeLayer, Settings

pytestmark = pytest.mark.integration

TOPICS_FILE = Path(__file__).parents[2] / "infra" / "local" / "kafka" / "topics.txt"


def declared_topics() -> set[str]:
    lines = TOPICS_FILE.read_text().splitlines()
    return {line.split()[0] for line in lines if line.strip() and not line.startswith("#")}


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings()


def test_kafka_has_every_declared_topic(settings: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka.bootstrap_servers})

    existing = set(admin.list_topics(timeout=10).topics)

    assert declared_topics() <= existing


def test_localstack_has_a_bucket_per_lake_layer(settings: Settings) -> None:
    s3 = boto3.client("s3", region_name=settings.aws.region, endpoint_url=settings.aws_endpoint_url)

    buckets = {bucket["Name"] for bucket in s3.list_buckets()["Buckets"]}

    assert {settings.lake.bucket_for(layer) for layer in LakeLayer} <= buckets
