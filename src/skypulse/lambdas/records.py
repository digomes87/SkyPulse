"""Helpers for SQS records delivered to Lambda."""

from collections.abc import Mapping
from typing import Any


def correlation_of(record: Mapping[str, Any]) -> str | None:
    attribute = record.get("messageAttributes", {}).get("correlation-id")
    return attribute.get("stringValue") if attribute else None
