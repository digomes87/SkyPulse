from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from skypulse.lambdas.claims import Claim, DynamoDbClaims
from skypulse.lambdas.eu261_calculator import process_batch
from tests.unit.lambdas.messages import disruption, sqs_record

ASSESSED_AT = datetime(2026, 10, 14, 19, 5, tzinfo=UTC)


class InMemoryClaims:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}

    def save_if_newer(self, claim: Claim) -> bool:
        existing = self.items.get(claim.item["claim_id"])
        if existing is not None and existing["detected_at"] >= claim.item["detected_at"]:
            return False
        self.items[claim.item["claim_id"]] = dict(claim.item)
        return True


def run(*records: dict[str, Any]) -> tuple[InMemoryClaims, dict[str, Any]]:
    claims = InMemoryClaims()
    response = process_batch(list(records), claims, lambda: ASSESSED_AT)
    return claims, response


def test_eligible_delay_is_stored_as_a_claim() -> None:
    claims, response = run(sqs_record(disruption()))

    claim = claims.items["EI105#2026-10-14T07:30:00Z"]
    assert response == {"batchItemFailures": []}
    assert claim["verdict"] == "eligible"
    assert claim["amount_eur"] == Decimal("600")
    assert claim["distance_band"] == "long"
    assert claim["correlation_id"] == "cid-42"


def test_duplicate_deliveries_store_one_claim() -> None:
    claims, _ = run(sqs_record(disruption(), "m-1"), sqs_record(disruption(), "m-2"))

    assert len(claims.items) == 1


def test_bad_records_are_reported_individually() -> None:
    broken = {"messageId": "m-bad", "body": "{not json"}

    claims, response = run(broken, sqs_record(disruption(), "m-good"))

    assert response == {"batchItemFailures": [{"itemIdentifier": "m-bad"}]}
    assert len(claims.items) == 1


def test_disruptions_without_reference_data_are_skipped_not_failed() -> None:
    unknown = {"icao": "ZZZZ", "country": None, "latitude": None, "longitude": None}

    claims, response = run(sqs_record(disruption(destination=unknown)))

    assert claims.items == {}
    assert response == {"batchItemFailures": []}


class ConditionalCheckFailedError(Exception):
    pass


class FakeTable:
    def __init__(self, reject: bool) -> None:
        self.reject = reject
        self.calls: list[dict[str, Any]] = []
        exceptions = type(
            "Exceptions", (), {"ConditionalCheckFailedException": ConditionalCheckFailedError}
        )
        self.meta = type("Meta", (), {"client": type("Client", (), {"exceptions": exceptions})})

    def put_item(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)
        if self.reject:
            raise ConditionalCheckFailedError


@pytest.mark.parametrize(("reject", "stored"), [(False, True), (True, False)])
def test_dynamodb_writes_are_conditional_on_detection_time(reject: bool, stored: bool) -> None:
    table = FakeTable(reject)
    claim = Claim({"claim_id": "EI105#x", "detected_at": "2026-10-14T19:00:00Z"})

    assert DynamoDbClaims(table).save_if_newer(claim) is stored
    assert "detected_at < :detected_at" in table.calls[0]["ConditionExpression"]
