"""flight_disruption envelopes as the detector publishes them."""

import json
from typing import Any

DUBLIN = {"icao": "EIDW", "country": "IE", "latitude": 53.4213, "longitude": -6.2701}
NEW_YORK = {"icao": "KJFK", "country": "US", "latitude": 40.6413, "longitude": -73.7781}
PALMA = {"icao": "LEPA", "country": "ES", "latitude": 39.5517, "longitude": 2.7388}
STANSTED = {"icao": "EGSS", "country": "GB", "latitude": 51.885, "longitude": 0.235}

STORM = {
    "observed_at": "2026-10-14T12:20:00Z",
    "present_weather": "+TSRA",
    "visibility_m": 1200,
    "wind_direction_degrees": 240,
    "wind_speed_kt": 28,
    "wind_gust_kt": 45,
}


def disruption(**overrides: Any) -> dict[str, Any]:
    data = {
        "flight_number": "EI105",
        "airline_icao": "EIN",
        "airline_country": "IE",
        "origin": DUBLIN,
        "destination": NEW_YORK,
        "scheduled_departure_utc": "2026-10-14T07:30:00Z",
        "scheduled_arrival_utc": "2026-10-14T14:30:00Z",
        "actual_departure_utc": "2026-10-14T11:40:00Z",
        "actual_arrival_utc": "2026-10-14T18:40:00Z",
        "kind": "arrival_delay",
        "status": "Arrived",
        "departure_delay_minutes": 250,
        "arrival_delay_minutes": 250,
        "detected_at": "2026-10-14T19:00:00Z",
        "origin_weather": [],
        "destination_weather": [],
        **overrides,
    }
    return {
        "event_id": "5b0f8f0e-4f7d-5a8e-9d0e-0c6c3b8b2f11",
        "event_type": "flight_disruption",
        "schema_version": 1,
        "source": "skypulse-detector",
        "produced_at": "2026-10-14T19:00:00Z",
        "data": data,
    }


def sqs_record(document: dict[str, Any], message_id: str = "m-1") -> dict[str, Any]:
    return {
        "messageId": message_id,
        "body": json.dumps(document),
        "messageAttributes": {
            "correlation-id": {"stringValue": "cid-42", "dataType": "String"},
        },
    }
