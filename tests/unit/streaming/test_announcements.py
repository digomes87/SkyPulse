import json
from typing import Any

import pytest

from skypulse.contracts import ContractViolationError
from skypulse.streaming.announcements import Announcement, SnsAnnouncer
from tests.unit.lambdas.messages import disruption


def payload(**overrides: Any) -> str:
    return json.dumps(disruption(**overrides)["data"])


class FakeSns:
    def __init__(self, failed: list[dict[str, str]] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.failed = failed or []

    def publish_batch(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"Successful": [], "Failed": self.failed}


def test_announcement_is_a_valid_event_keyed_on_flight_and_kind() -> None:
    first = Announcement.from_payload(payload(), "cid-1")
    again = Announcement.from_payload(payload(), "cid-2")
    other_kind = Announcement.from_payload(payload(kind="departure_delay"), "cid-1")

    assert first.event.event_id == again.event.event_id
    assert first.event.event_id != other_kind.event.event_id


def test_invalid_payloads_are_rejected_before_publishing() -> None:
    with pytest.raises(ContractViolationError, match="kind"):
        Announcement.from_payload(payload(kind="rumour"), None)


def test_sns_entries_carry_kind_and_correlation_attributes() -> None:
    entry = Announcement.from_payload(payload(), "cid-1").sns_entry()

    assert entry["MessageAttributes"]["kind"]["StringValue"] == "arrival_delay"
    assert entry["MessageAttributes"]["correlation-id"]["StringValue"] == "cid-1"
    assert json.loads(entry["Message"])["event_type"] == "flight_disruption"


def test_announcer_respects_the_sns_batch_limit() -> None:
    sns = FakeSns()
    items = [Announcement.from_payload(payload(flight_number=f"FR{n}"), None) for n in range(1, 24)]

    sent = SnsAnnouncer(sns, "arn:aws:sns:eu-west-1:000000000000:flight-disruptions").announce(
        items
    )

    assert sent == 23
    assert [len(call["PublishBatchRequestEntries"]) for call in sns.calls] == [10, 10, 3]


def test_announcer_fails_loudly_on_rejected_messages() -> None:
    sns = FakeSns(failed=[{"Id": "x", "Code": "InternalError"}])
    item = Announcement.from_payload(payload(), None)

    with pytest.raises(RuntimeError, match="rejected 1"):
        SnsAnnouncer(sns, "arn").announce([item])
