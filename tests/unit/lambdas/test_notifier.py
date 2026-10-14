import json
from collections.abc import Mapping
from typing import Any

import pytest

from skypulse.lambdas.notifier import process_batch
from tests.unit.lambdas.messages import disruption, sqs_record


def test_each_disruption_is_logged_and_sent(capsys: pytest.CaptureFixture[str]) -> None:
    sent: list[Mapping[str, Any]] = []

    response = process_batch([sqs_record(disruption(kind="departure_delay"))], sent.append)

    assert response == {"batchItemFailures": []}
    assert sent[0]["route"] == "EIDW-KJFK"
    assert sent[0]["kind"] == "departure_delay"
    line = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert line["event"] == "disruption_notified"
    assert line["correlation_id"] == "cid-42"


def test_webhook_failures_are_retried_through_the_queue() -> None:
    def failing(_: Mapping[str, Any]) -> None:
        raise ConnectionError("webhook down")

    response = process_batch([sqs_record(disruption(), "m-7")], failing)

    assert response == {"batchItemFailures": [{"itemIdentifier": "m-7"}]}


def test_without_webhook_only_logs() -> None:
    assert process_batch([sqs_record(disruption())], None) == {"batchItemFailures": []}
