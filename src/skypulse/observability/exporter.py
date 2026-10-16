"""Metrics about state rather than processes: ``python -m skypulse.observability.exporter``.

Every interval it records when each lake table last changed (and whether that breaks its
freshness SLA), how many messages wait in the SQS dead-letter queues and the Kafka
dead-letter topic, and how many EU261 claims are stored.
"""

import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

import boto3
from confluent_kafka import Consumer, TopicPartition
from prometheus_client import Gauge

from skypulse.common.config import Settings
from skypulse.common.logging import configure_logging, get_logger
from skypulse.ingestion.publisher import DEFAULT_ROUTES
from skypulse.observability.lake import LAKE_TABLES, last_commit
from skypulse.observability.metrics import start_metrics_server
from skypulse.quality.results import Status
from skypulse.quality.statistics import FRESHNESS_SLAS, evaluate_freshness

DEAD_LETTER_QUEUES = ("eu261-calculator-dlq", "notifications-dlq")
CLAIMS_TABLE = "eu261-claims"
INTERVAL_SECONDS = 30
DEFAULT_PORT = 9465

LAST_COMMIT = Gauge(
    "skypulse_table_last_commit_timestamp_seconds", "Last Delta commit", ["layer", "table"]
)
SLA_BREACHED = Gauge("skypulse_table_freshness_sla_breached", "1 when stale", ["table"])
DLQ_MESSAGES = Gauge("skypulse_dlq_messages", "Messages waiting in an SQS DLQ", ["queue"])
TOPIC_MESSAGES = Gauge("skypulse_kafka_topic_messages", "Messages retained in a topic", ["topic"])
CLAIMS = Gauge("skypulse_eu261_claims", "Claims stored in DynamoDB")

_log = get_logger(__name__)


def record_lake(settings: Settings, now: datetime) -> None:
    commits = {table.name: last_commit(table, settings) for table in LAKE_TABLES}
    for table in LAKE_TABLES:
        moment = commits[table.name]
        if moment is not None:
            LAST_COMMIT.labels(str(table.layer), table.name).set(moment.timestamp())
    for sla in FRESHNESS_SLAS:
        result = evaluate_freshness(sla, commits.get(sla.table), now)
        SLA_BREACHED.labels(sla.table).set(1 if result.status is Status.FAIL else 0)


def record_queues(sqs: Any, queues: Sequence[str]) -> None:
    for name in queues:
        url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
        attributes = sqs.get_queue_attributes(
            QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
        )["Attributes"]
        DLQ_MESSAGES.labels(name).set(int(attributes["ApproximateNumberOfMessages"]))


def record_topic(consumer: Any, topic: str) -> None:
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    retained = 0
    for partition in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        retained += high - low
    TOPIC_MESSAGES.labels(topic).set(retained)


def record_claims(dynamodb: Any) -> None:
    CLAIMS.set(dynamodb.describe_table(TableName=CLAIMS_TABLE)["Table"]["ItemCount"])


class Exporter:
    def __init__(self, settings: Settings, sqs: Any, dynamodb: Any, consumer: Any) -> None:
        self._settings = settings
        self._sqs = sqs
        self._dynamodb = dynamodb
        self._consumer = consumer

    @classmethod
    def from_settings(cls, settings: Settings) -> "Exporter":
        options = {"endpoint_url": settings.aws_endpoint_url, "region_name": settings.aws.region}
        consumer = Consumer(
            {"bootstrap.servers": settings.kafka.bootstrap_servers, "group.id": "skypulse-exporter"}
        )
        return cls(
            settings, boto3.client("sqs", **options), boto3.client("dynamodb", **options), consumer
        )

    def collect_once(self) -> None:
        probes: dict[str, Callable[[], None]] = {
            "lake": lambda: record_lake(self._settings, datetime.now(UTC)),
            "queues": lambda: record_queues(self._sqs, DEAD_LETTER_QUEUES),
            "topic": lambda: record_topic(self._consumer, str(DEFAULT_ROUTES.dead_letter)),
            "claims": lambda: record_claims(self._dynamodb),
        }
        for name, probe in probes.items():
            try:
                probe()
            except Exception:  # one unreachable dependency must not blank the others
                _log.exception("exporter_probe_failed", probe=name)


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    start_metrics_server(settings, default_port=DEFAULT_PORT)
    exporter = Exporter.from_settings(settings)
    while True:
        exporter.collect_once()
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
