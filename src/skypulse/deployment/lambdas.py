"""Packages the Lambda handlers and deploys them to LocalStack.

``python -m skypulse.deployment.lambdas`` builds one zip with only the standard-library
code the handlers need (``skypulse.eu261`` and ``skypulse.lambdas``), creates or updates
both functions and wires them to their SQS queues with partial batch responses.
"""

import io
import time
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3

from skypulse.common.config import Settings
from skypulse.common.logging import configure_logging, get_logger

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
BUNDLED = ("__init__.py", "eu261", "lambdas")
RUNTIME = "python3.12"
LOCAL_ROLE = "arn:aws:iam::000000000000:role/skypulse-lambda"
TIMEOUT_SECONDS = 30
BATCH_SIZE = 10
ACTIVE_POLL_SECONDS = 2
ACTIVE_ATTEMPTS = 60

_log = get_logger(__name__)


@dataclass(frozen=True)
class FunctionSpec:
    name: str
    handler: str
    queue: str
    environment: Mapping[str, str]


FUNCTIONS = (
    FunctionSpec(
        "eu261-calculator",
        "skypulse.lambdas.eu261_calculator.handler",
        "eu261-calculator",
        {"CLAIMS_TABLE": "eu261-claims"},
    ),
    FunctionSpec("notifier", "skypulse.lambdas.notifier.handler", "notifications", {}),
)


def build_zip() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for entry in BUNDLED:
            source = PACKAGE_ROOT / entry
            files = [source] if source.is_file() else sorted(source.rglob("*.py"))
            for path in files:
                archive.write(path, f"skypulse/{path.relative_to(PACKAGE_ROOT)}")
    return buffer.getvalue()


class LambdaDeployer:
    def __init__(self, lambdas: Any, sqs: Any) -> None:
        self._lambdas = lambdas
        self._sqs = sqs

    def deploy(self, spec: FunctionSpec, code: bytes) -> None:
        self._create_or_update(spec, code)
        self._wait_until_active(spec.name)
        self._connect_queue(spec)
        _log.info("lambda_deployed", function=spec.name, queue=spec.queue)

    def _create_or_update(self, spec: FunctionSpec, code: bytes) -> None:
        try:
            self._lambdas.create_function(
                FunctionName=spec.name,
                Runtime=RUNTIME,
                Role=LOCAL_ROLE,
                Handler=spec.handler,
                Code={"ZipFile": code},
                Timeout=TIMEOUT_SECONDS,
                Environment={"Variables": dict(spec.environment)},
            )
        except self._lambdas.exceptions.ResourceConflictException:
            self._lambdas.update_function_code(FunctionName=spec.name, ZipFile=code)

    def _wait_until_active(self, name: str) -> None:
        for _ in range(ACTIVE_ATTEMPTS):
            configuration = self._lambdas.get_function_configuration(FunctionName=name)
            if (
                configuration["State"] == "Active"
                and configuration.get("LastUpdateStatus") != "InProgress"
            ):
                return
            time.sleep(ACTIVE_POLL_SECONDS)
        raise TimeoutError(f"lambda {name} did not become active")

    def _connect_queue(self, spec: FunctionSpec) -> None:
        queue_url = self._sqs.get_queue_url(QueueName=spec.queue)["QueueUrl"]
        queue_arn = self._sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        existing = self._lambdas.list_event_source_mappings(
            FunctionName=spec.name, EventSourceArn=queue_arn
        )
        if existing["EventSourceMappings"]:
            return
        self._lambdas.create_event_source_mapping(
            FunctionName=spec.name,
            EventSourceArn=queue_arn,
            BatchSize=BATCH_SIZE,
            FunctionResponseTypes=["ReportBatchItemFailures"],
        )


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    endpoint = settings.aws_endpoint_url
    region = settings.aws.region
    deployer = LambdaDeployer(
        boto3.client("lambda", endpoint_url=endpoint, region_name=region),
        boto3.client("sqs", endpoint_url=endpoint, region_name=region),
    )
    code = build_zip()
    for spec in FUNCTIONS:
        deployer.deploy(spec, code)


if __name__ == "__main__":
    main()
