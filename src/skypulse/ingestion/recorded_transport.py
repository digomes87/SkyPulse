"""An httpx transport that answers OpenSky and aviationweather.gov calls from fixtures.

The real clients run unchanged on top of it, so replay mode covers parsing, retries and
publishing exactly as in production. Layout under the fixtures root:

- ``opensky/states/<snapshot>.json[.gz]``: raw /states/all bodies
- ``metar/<snapshot>.json[.gz]``: raw /api/data/metar bodies (JSON format)
"""

from collections.abc import Callable
from pathlib import Path

import httpx

from skypulse.common.clock import Clock
from skypulse.ingestion.snapshots import SnapshotFolder

Responder = Callable[[httpx.Request], httpx.Response]


def recorded_transport(root: Path, clock: Clock) -> httpx.MockTransport:
    routes: dict[tuple[str, str], Responder] = {
        ("opensky-network.org", "/api/states/all"): _states(
            SnapshotFolder(root / "opensky/states"), clock
        ),
        ("aviationweather.gov", "/api/data/metar"): _metar(SnapshotFolder(root / "metar"), clock),
    }

    def handle(request: httpx.Request) -> httpx.Response:
        responder = routes.get((request.url.host, request.url.path))
        if responder is None:
            return httpx.Response(404)
        return responder(request)

    return httpx.MockTransport(handle)


def _states(folder: SnapshotFolder, clock: Clock) -> Responder:
    def respond(_: httpx.Request) -> httpx.Response:
        now = clock.now()
        snapshot = folder.latest_at(now)
        if snapshot is None:
            return httpx.Response(200, json={"time": int(now.timestamp()), "states": []})
        return httpx.Response(200, json=snapshot.load())

    return respond


def _metar(folder: SnapshotFolder, clock: Clock) -> Responder:
    def respond(request: httpx.Request) -> httpx.Response:
        snapshot = folder.latest_at(clock.now())
        if snapshot is None:
            return httpx.Response(204)
        wanted = set(request.url.params.get("ids", "").split(","))
        return httpx.Response(
            200, json=[item for item in snapshot.load() if item["icaoId"] in wanted]
        )

    return respond
