"""SQS-triggered Lambda: assess each flight_disruption and store the EU261 claim.

Uses partial batch responses: only the records that failed are returned to the queue,
and after three receives SQS moves them to the dead-letter queue.
"""

import json
import os
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import boto3  # provided by the Lambda runtime

from skypulse.lambdas.claims import Claim, ClaimRepository, DynamoDbClaims
from skypulse.lambdas.disruption import Disruption
from skypulse.lambdas.logs import log

CLAIMS_TABLE_VARIABLE = "CLAIMS_TABLE"
DEFAULT_CLAIMS_TABLE = "eu261-claims"

Clock = Callable[[], datetime]


def correlation_of(record: Mapping[str, Any]) -> str | None:
    attribute = record.get("messageAttributes", {}).get("correlation-id")
    return attribute.get("stringValue") if attribute else None


def process_record(record: Mapping[str, Any], claims: ClaimRepository, clock: Clock) -> None:
    correlation_id = correlation_of(record)
    disruption = Disruption.from_document(json.loads(record["body"]))
    if not disruption.assessable():
        log("claim_skipped", reason="missing reference data", claim_id=disruption.claim_id(),
            correlation_id=correlation_id)  # fmt: skip
        return
    assessment = disruption.assess()
    claim = Claim.from_assessment(disruption, assessment, clock(), correlation_id)
    stored = claims.save_if_newer(claim)
    log(
        "claim_assessed",
        claim_id=disruption.claim_id(),
        verdict=str(assessment.verdict),
        amount_eur=assessment.compensation.amount_eur,
        extraordinary_circumstances_likely=assessment.extraordinary_circumstances_likely(),
        stored=stored,
        correlation_id=correlation_id,
    )


def process_batch(
    records: list[Mapping[str, Any]], claims: ClaimRepository, clock: Clock
) -> dict[str, list[dict[str, str]]]:
    failures = []
    for record in records:
        try:
            process_record(record, claims, clock)
        except Exception as error:
            log("claim_failed", level="error", message_id=record["messageId"], error=repr(error),
                correlation_id=correlation_of(record))  # fmt: skip
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}


def handler(event: Mapping[str, Any], _context: object) -> dict[str, list[dict[str, str]]]:
    table = boto3.resource("dynamodb").Table(
        os.environ.get(CLAIMS_TABLE_VARIABLE, DEFAULT_CLAIMS_TABLE)
    )
    return process_batch(
        list(event.get("Records", [])), DynamoDbClaims(table), lambda: datetime.now(UTC)
    )
