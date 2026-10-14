import json
import uuid
from datetime import UTC, datetime

import pytest

from skypulse.contracts import (
    ContractViolationError,
    Event,
    EventSource,
    EventType,
    contract_for,
)

PRODUCED_AT = datetime(2026, 10, 14, 9, 30, tzinfo=UTC)

VALID_METAR = {
    "station_icao": "EIDW",
    "observed_at": "2026-10-14T09:30:00Z",
    "report_type": "METAR",
    "wind_direction_degrees": 190,
    "wind_variable": False,
    "wind_speed_kt": 15,
    "wind_gust_kt": 26,
    "visibility_m": 9999,
    "present_weather": None,
    "temperature_c": 15,
    "dewpoint_c": 10,
    "altimeter_hpa": 1019,
    "flight_category": "VFR",
    "raw_text": "METAR EIDW 140930Z 19015G26KT 9999 FEW022 15/10 Q1019 NOSIG",
}


def metar_event(data: dict[str, object] | None = None) -> Event:
    return Event.create(
        EventType.METAR_OBSERVATION,
        EventSource.AVIATIONWEATHER,
        natural_key="EIDW|2026-10-14T09:30:00Z|METAR",
        data=data or VALID_METAR,
        produced_at=PRODUCED_AT,
    )


@pytest.mark.parametrize("event_type", list(EventType))
def test_every_event_type_has_a_contract_with_the_shared_envelope(event_type: EventType) -> None:
    contract = contract_for(event_type)

    assert contract.schema["properties"]["event_type"] == {"const": event_type.value}
    assert set(contract.schema["required"]) == {
        "event_id",
        "event_type",
        "schema_version",
        "source",
        "produced_at",
        "data",
    }


def test_event_id_is_deterministic_for_the_same_natural_key() -> None:
    assert metar_event().event_id == metar_event().event_id
    assert uuid.UUID(metar_event().event_id).version == 5


def test_event_serialises_to_the_envelope_and_validates() -> None:
    event = metar_event()

    document = json.loads(event.to_json())

    assert document["event_type"] == "metar_observation"
    assert document["schema_version"] == 1
    assert document["source"] == "aviationweather"
    assert document["produced_at"] == "2026-10-14T09:30:00Z"
    contract_for(EventType.METAR_OBSERVATION).validate(event)


def test_validation_reports_the_offending_field() -> None:
    broken = metar_event({**VALID_METAR, "station_icao": "dublin"})

    with pytest.raises(ContractViolationError, match="station_icao"):
        contract_for(EventType.METAR_OBSERVATION).validate(broken)


def test_validation_rejects_unknown_fields() -> None:
    extended = metar_event({**VALID_METAR, "surprise": 1})

    with pytest.raises(ContractViolationError, match="surprise"):
        contract_for(EventType.METAR_OBSERVATION).validate(extended)


def test_produced_at_must_be_timezone_aware() -> None:
    with pytest.raises(ValueError, match="timezone"):
        Event.create(
            EventType.METAR_OBSERVATION,
            EventSource.REPLAY,
            natural_key="k",
            data=VALID_METAR,
            produced_at=datetime(2026, 10, 14, 9, 30),  # noqa: DTZ001
        )


def test_validation_checks_date_time_formats() -> None:
    malformed = metar_event({**VALID_METAR, "observed_at": "yesterday morning"})

    with pytest.raises(ContractViolationError, match="observed_at"):
        contract_for(EventType.METAR_OBSERVATION).validate(malformed)
