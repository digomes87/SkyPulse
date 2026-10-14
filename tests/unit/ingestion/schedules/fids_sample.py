"""Hand-written AeroDataBox FIDS payloads (withLeg=true) used across schedule tests."""

from typing import Any


def departure(
    number: str = "FR 7241",
    status: str = "Departed",
    destination: dict[str, Any] | None = None,
    revised: str | None = "2026-10-14 07:20Z",
) -> dict[str, Any]:
    departure_leg: dict[str, Any] = {
        "scheduledTime": {"utc": "2026-10-14 06:55Z", "local": "2026-10-14 07:55+01:00"},
        "quality": ["Basic", "Live"],
    }
    if revised:
        departure_leg["revisedTime"] = {"utc": revised, "local": "2026-10-14 08:20+01:00"}
    return {
        "departure": departure_leg,
        "arrival": {
            "airport": destination or {"icao": "LEMD", "iata": "MAD", "name": "Madrid"},
            "scheduledTime": {"utc": "2026-10-14 09:30Z", "local": "2026-10-14 11:30+02:00"},
            "quality": ["Basic"],
        },
        "number": number,
        "callSign": "RYR7241",
        "status": status,
        "codeshareStatus": "IsOperator",
        "isCargo": False,
        "airline": {"name": "Ryanair", "iata": "FR", "icao": "RYR"},
    }


def arrival() -> dict[str, Any]:
    return {
        "departure": {
            "airport": {"icao": "LEMD", "iata": "MAD", "name": "Madrid"},
            "scheduledTime": {"utc": "2026-10-14 10:15Z", "local": "2026-10-14 12:15+02:00"},
            "quality": ["Basic"],
        },
        "arrival": {
            "scheduledTime": {"utc": "2026-10-14 12:50Z", "local": "2026-10-14 13:50+01:00"},
            "runwayTime": {"utc": "2026-10-14 16:05Z", "local": "2026-10-14 17:05+01:00"},
            "quality": ["Basic", "Live"],
        },
        "number": "FR 7242",
        "status": "Arrived",
        "codeshareStatus": "IsOperator",
        "isCargo": False,
        "airline": {"name": "Ryanair", "iata": "FR", "icao": "RYR"},
    }


def fids(*departures: dict[str, Any], arrivals: tuple[dict[str, Any], ...] = ()) -> dict[str, Any]:
    return {"departures": list(departures), "arrivals": list(arrivals)}
