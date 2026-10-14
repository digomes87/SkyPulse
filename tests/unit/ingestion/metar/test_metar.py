from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from skypulse.common.aviation import AirportIcao
from skypulse.common.http import RetryingHttp, RetryPolicy
from skypulse.contracts import EventSource, EventType, contract_for
from skypulse.ingestion.metar.client import METAR_URL, AviationWeatherClient
from skypulse.ingestion.metar.models import MetarObservation

PRODUCED_AT = datetime(2026, 10, 14, 10, 0, tzinfo=UTC)

DUBLIN: dict[str, Any] = {
    "icaoId": "EIDW",
    "obsTime": 1791970200,
    "metarType": "METAR",
    "temp": 15,
    "dewp": 10,
    "wdir": 190,
    "wspd": 15,
    "wgst": 26,
    "visib": "6+",
    "altim": 1019,
    "rawOb": "METAR EIDW 140930Z 19015G26KT 070V220 9999 FEW022 15/10 Q1019 NOSIG",
    "fltCat": "VFR",
}


def test_parses_an_observation() -> None:
    data = MetarObservation.from_payload(DUBLIN).data

    assert data["station_icao"] == "EIDW"
    assert data["observed_at"] == "2026-10-14T09:30:00Z"
    assert data["wind_direction_degrees"] == 190
    assert data["wind_gust_kt"] == 26
    assert data["visibility_m"] == 9999
    assert data["present_weather"] is None


@pytest.mark.parametrize(
    ("visib", "metres"),
    [("6+", 9999), ("10+", 9999), (6, 9656), (0.25, 402), (1.5, 2414), (None, None)],
)
def test_visibility_is_converted_from_statute_miles(visib: object, metres: int | None) -> None:
    data = MetarObservation.from_payload({**DUBLIN, "visib": visib}).data

    assert data["visibility_m"] == metres


def test_variable_wind_has_no_direction() -> None:
    data = MetarObservation.from_payload({**DUBLIN, "wdir": "VRB", "wspd": 3}).data

    assert data["wind_direction_degrees"] is None
    assert data["wind_variable"] is True


def test_present_weather_is_kept() -> None:
    data = MetarObservation.from_payload({**DUBLIN, "wxString": "+TSRA BR"}).data

    assert data["present_weather"] == "+TSRA BR"


def test_event_validates_and_is_keyed_on_station_and_time() -> None:
    event = MetarObservation.from_payload(DUBLIN).to_event(EventSource.AVIATIONWEATHER, PRODUCED_AT)

    contract_for(EventType.METAR_OBSERVATION).validate(event)
    again = MetarObservation.from_payload(DUBLIN).to_event(EventSource.REPLAY, PRODUCED_AT)
    assert event.event_id == again.event_id


async def test_client_batches_stations_and_sends_a_user_agent() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=[DUBLIN])

    http = RetryingHttp(httpx.AsyncClient(transport=httpx.MockTransport(handler)), RetryPolicy())
    client = AviationWeatherClient(http, "SkyPulse-test/1.0", batch_size=2)
    stations = [AirportIcao(code) for code in ("EIDW", "EGLL", "LEMD")]

    observations = await client.observations(stations, hours=2)

    assert len(observations) == 2
    assert [request.url.params["ids"] for request in seen] == ["EIDW,EGLL", "LEMD"]
    assert seen[0].url.params["format"] == "json"
    assert seen[0].url.params["hours"] == "2"
    assert seen[0].headers["User-Agent"] == "SkyPulse-test/1.0"
    assert str(seen[0].url).startswith(METAR_URL)


async def test_client_handles_no_content() -> None:
    http = RetryingHttp(
        httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(204))),
        RetryPolicy(),
    )

    observations = await AviationWeatherClient(http, "ua").observations([AirportIcao("EIDW")], 1)

    assert len(observations) == 0
