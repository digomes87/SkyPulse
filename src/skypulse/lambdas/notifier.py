"""SQS-triggered Lambda: turn disruptions into notifications.

Every disruption becomes a structured log line. When ``WEBHOOK_URL`` is set the same
payload is POSTed there (for example a Slack incoming webhook or a test endpoint).
"""

import json
import os
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from typing import Any

from skypulse.lambdas.disruption import Disruption
from skypulse.lambdas.logs import log
from skypulse.lambdas.records import correlation_of

WEBHOOK_TIMEOUT_SECONDS = 5

Sender = Callable[[Mapping[str, Any]], None]


def notification(disruption: Disruption) -> dict[str, Any]:
    data = disruption.data
    return {
        "flight": data["flight_number"],
        "route": f"{data['origin']['icao']}-{data['destination']['icao']}",
        "kind": data["kind"],
        "status": data["status"],
        "departure_delay_minutes": data["departure_delay_minutes"],
        "arrival_delay_minutes": data["arrival_delay_minutes"],
        "scheduled_departure_utc": data["scheduled_departure_utc"],
    }


ALLOWED_SCHEMES = ("https", "http")


def webhook_sender(url: str) -> Sender:
    if urllib.parse.urlparse(url).scheme not in ALLOWED_SCHEMES:
        raise ValueError(f"webhook must be http(s): {url!r}")

    def send(payload: Mapping[str, Any]) -> None:
        body = json.dumps(payload).encode()
        request = urllib.request.Request(  # noqa: S310 - scheme checked above
            url, data=body, headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(request, timeout=WEBHOOK_TIMEOUT_SECONDS):  # noqa: S310
            return

    return send


def process_batch(
    records: list[Mapping[str, Any]], send: Sender | None
) -> dict[str, list[dict[str, str]]]:
    failures = []
    for record in records:
        try:
            payload = notification(Disruption.from_document(json.loads(record["body"])))
            log("disruption_notified", correlation_id=correlation_of(record), **payload)
            if send is not None:
                send(payload)
        except Exception as error:
            log(
                "notification_failed",
                level="error",
                message_id=record["messageId"],
                error=repr(error),
            )
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}


def handler(event: Mapping[str, Any], _context: object) -> dict[str, list[dict[str, str]]]:
    url = os.environ.get("WEBHOOK_URL")
    return process_batch(list(event.get("Records", [])), webhook_sender(url) if url else None)
