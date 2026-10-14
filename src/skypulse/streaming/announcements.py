"""Publishing flight_disruption events to SNS.

Message attributes carry the correlation id and the disruption kind; the EU261 calculator
subscription filters on ``kind`` so early-warning departure delays only reach notifications.
"""

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from itertools import batched
from typing import Any, Protocol

from skypulse.contracts import Event, EventSource, EventType, contract_for

SNS_BATCH_LIMIT = 10


@dataclass(frozen=True)
class Announcement:
    event: Event
    correlation_id: str | None

    @classmethod
    def from_payload(cls, payload: str, correlation_id: str | None) -> "Announcement":
        data: dict[str, Any] = json.loads(payload)
        key = f"{data['flight_number']}|{data['scheduled_departure_utc']}|{data['kind']}"
        detected_at = datetime.fromisoformat(data["detected_at"])
        event = Event.create(
            EventType.FLIGHT_DISRUPTION, EventSource.DETECTOR, key, data, detected_at
        )
        contract_for(EventType.FLIGHT_DISRUPTION).validate(event)
        return cls(event, correlation_id)

    def sns_entry(self) -> dict[str, Any]:
        attributes = {"kind": {"DataType": "String", "StringValue": self.event.data["kind"]}}
        if self.correlation_id:
            attributes["correlation-id"] = {
                "DataType": "String",
                "StringValue": self.correlation_id,
            }
        return {
            "Id": self.event.event_id.replace("-", ""),
            "Message": self.event.to_json().decode(),
            "MessageAttributes": attributes,
        }


class Announcer(Protocol):
    def announce(self, announcements: Iterable[Announcement]) -> int: ...


class SnsAnnouncer:
    def __init__(self, client: Any, topic_arn: str) -> None:
        self._client = client
        self._topic_arn = topic_arn

    def announce(self, announcements: Iterable[Announcement]) -> int:
        sent = 0
        for group in batched(announcements, SNS_BATCH_LIMIT):
            response: Mapping[str, Any] = self._client.publish_batch(
                TopicArn=self._topic_arn,
                PublishBatchRequestEntries=[item.sns_entry() for item in group],
            )
            failed = response.get("Failed", [])
            if failed:
                raise RuntimeError(f"SNS rejected {len(failed)} disruption messages: {failed}")
            sent += len(group)
        return sent
