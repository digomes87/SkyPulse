"""Event envelope and JSON Schema contracts (see contracts/README.md)."""

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from functools import cache
from importlib import resources
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import best_match

from skypulse.common.clock import require_aware, utc_iso

SCHEMA_VERSION = 1

# Fixed namespace so event ids are stable across processes, hosts and releases.
EVENT_NAMESPACE = uuid.UUID("6f1c2b0e-8a8f-5d2c-9b1a-5c7e0d9a4f10")

_REPOSITORY_CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"


class EventType(StrEnum):
    AIRCRAFT_STATE = "aircraft_state"
    FLIGHT_SCHEDULE = "flight_schedule"
    METAR_OBSERVATION = "metar_observation"

    @property
    def contract_file(self) -> str:
        return f"{self.value.replace('_', '-')}.v{SCHEMA_VERSION}.json"


class EventSource(StrEnum):
    OPENSKY = "opensky"
    AERODATABOX = "aerodatabox"
    AVIATIONWEATHER = "aviationweather"
    REPLAY = "replay"


class ContractViolationError(ValueError):
    pass


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: EventType
    source: EventSource
    produced_at: datetime
    data: Mapping[str, Any]

    @classmethod
    def create(
        cls,
        event_type: EventType,
        source: EventSource,
        natural_key: str,
        data: Mapping[str, Any],
        produced_at: datetime,
    ) -> "Event":
        event_id = uuid.uuid5(EVENT_NAMESPACE, f"{event_type}|{natural_key}")
        return cls(str(event_id), event_type, source, require_aware(produced_at), data)

    def to_document(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "schema_version": SCHEMA_VERSION,
            "source": self.source.value,
            "produced_at": utc_iso(self.produced_at),
            "data": dict(self.data),
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_document(), separators=(",", ":")).encode()


@dataclass(frozen=True)
class Contract:
    schema: Mapping[str, Any]

    def validate(self, event: Event) -> None:
        validator = Draft202012Validator(
            self.schema, format_checker=Draft202012Validator.FORMAT_CHECKER
        )
        error = best_match(validator.iter_errors(event.to_document()))
        if error is None:
            return
        location = ".".join(str(part) for part in error.absolute_path) or "<root>"
        raise ContractViolationError(f"{event.event_type} invalid at {location}: {error.message}")


@cache
def contract_for(event_type: EventType) -> Contract:
    return Contract(json.loads(_read_contract(event_type.contract_file)))


def _read_contract(file_name: str) -> str:
    """Contracts ship inside the wheel; in a source checkout they are read from the repo."""
    packaged = resources.files("skypulse").joinpath("_contracts", file_name)
    if packaged.is_file():
        return packaged.read_text()
    return (_REPOSITORY_CONTRACTS / file_name).read_text()
