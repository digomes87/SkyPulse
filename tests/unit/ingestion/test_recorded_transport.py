import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx

from skypulse.common.clock import FrozenClock
from skypulse.ingestion.recorded_transport import recorded_transport

NOW = datetime(2026, 10, 14, 10, 30, tzinfo=UTC)


def write(path: Path, payload: object, compress: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload).encode()
    path.write_bytes(gzip.compress(raw) if compress else raw)


async def get(root: Path, url: str) -> httpx.Response:
    transport = recorded_transport(root, FrozenClock(NOW))
    async with httpx.AsyncClient(transport=transport) as client:
        return await client.get(url)


async def test_serves_the_latest_states_snapshot_before_the_clock(tmp_path: Path) -> None:
    write(tmp_path / "opensky/states/20261014T1000Z.json.gz", {"time": 1, "states": []}, True)
    write(tmp_path / "opensky/states/20261014T1100Z.json.gz", {"time": 2, "states": []}, True)

    response = await get(tmp_path, "https://opensky-network.org/api/states/all?lamin=1")

    assert response.json()["time"] == 1


async def test_states_before_the_first_snapshot_are_empty(tmp_path: Path) -> None:
    response = await get(tmp_path, "https://opensky-network.org/api/states/all")

    assert response.json()["states"] == []


async def test_metar_is_filtered_to_the_requested_stations(tmp_path: Path) -> None:
    observations = [{"icaoId": "EIDW"}, {"icaoId": "EGLL"}, {"icaoId": "LEMD"}]
    write(tmp_path / "metar/20261014T1000Z.json", observations)

    response = await get(tmp_path, "https://aviationweather.gov/api/data/metar?ids=EIDW,LEMD")

    assert [item["icaoId"] for item in response.json()] == ["EIDW", "LEMD"]


async def test_metar_without_snapshot_is_no_content(tmp_path: Path) -> None:
    response = await get(tmp_path, "https://aviationweather.gov/api/data/metar?ids=EIDW")

    assert response.status_code == 204


async def test_unknown_endpoints_are_not_found(tmp_path: Path) -> None:
    response = await get(tmp_path, "https://example.test/anything")

    assert response.status_code == 404
