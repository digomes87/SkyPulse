# Data sources

Checked against each provider's documentation on 2026-10-14. Limits change; re-check before
relying on them in production.

## OpenSky Network REST API

- **Docs:** https://openskynetwork.github.io/opensky-api/rest.html
- **Base URL:** `https://opensky-network.org/api`
- **Auth:** OAuth2 client credentials only (basic auth has been retired).
  Token endpoint:
  `https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token`.
  Tokens last 30 minutes; SkyPulse refreshes 30 seconds early and retries once on a 401
  (`skypulse.ingestion.opensky.access`).
- **Anonymous access:** allowed for `/states/all` only. `/flights/*` returns 403 without a token.
- **Credits:** daily budget per endpoint family. Anonymous 400, registered 4,000, active
  feeders 8,000. `/states/all` costs by bounding-box area:

  | Area (sq deg) | Credits |
  |---------------|---------|
  | <= 25 | 1 |
  | 25-100 | 2 |
  | 100-400 | 3 |
  | > 400 or global | 4 |

  `/flights/*` costs 4 credits for recent data.
- **Rate-limit headers:** `X-Rate-Limit-Remaining` (balance) and, on 429,
  `X-Rate-Limit-Retry-After-Seconds`. SkyPulse tracks the balance, refuses calls it cannot
  afford, honours the retry header and backs off 15 minutes when credits run out.
- **Endpoints used:**

  | Endpoint | Purpose | Notes |
  |----------|---------|-------|
  | `/states/all?lamin&lomin&lamax&lomax` | Live positions | 17 fields per vector (18 with `extended=1`); `states` is `null` when empty |
  | `/flights/departure?airport&begin&end` | Observed departures | Interval at most 2 days; 404 means no flights |
  | `/flights/arrival?airport&begin&end` | Observed arrivals | Same; filled by a nightly batch, so only previous days |

- **Terms:** data is free for research and non-commercial use; commercial use needs a
  licence. Cite the OpenSky Network when publishing results.

## AeroDataBox (via RapidAPI)

- **Docs:** https://doc.aerodatabox.com/ (OpenAPI: `/docs/openapi-rapidapi-v1.yaml`)
- **Base URL:** `https://aerodatabox.p.rapidapi.com`
- **Auth:** headers `X-RapidAPI-Key: <key>` and `X-RapidAPI-Host: aerodatabox.p.rapidapi.com`.
- **Endpoint used:** `GET /flights/airports/icao/{icao}/{fromLocal}/{toLocal}` (FIDS, tier 2),
  with `withLeg=true&direction=Both&withCancelled=true&withCodeshared=false`.
  - `fromLocal` / `toLocal` are **airport local time** (`YYYY-MM-DDTHH:mm`); the maximum span
    depends on the plan. SkyPulse sends 12-hour windows.
  - Times in the body are given as `{"utc": "2026-10-14 06:55Z", "local": "..."}`.
  - The leg at the requested airport carries no `airport` object.
  - 204 means no flights in the window.
- **Pricing and quotas:** calls are priced per endpoint tier and plan; the free tier on
  RapidAPI is small. See https://aerodatabox.com/pricing. Demo mode does not call it at all.
- **Coverage:** live updates (revised and runway times) exist only for airports with live
  coverage; others return schedules only.
- **Terms:** https://aerodatabox.com/terms (accepted on subscription).

## aviationweather.gov Data API (METAR)

- **Docs:** https://aviationweather.gov/data/api/ (OpenAPI: `/data/schema/openapi.yaml`)
- **Endpoint used:** `GET https://aviationweather.gov/api/data/metar?ids=EIDW,EGLL&format=json&hours=2`
- **Auth:** none. Operators ask for a descriptive `User-Agent`; SkyPulse sends
  `SkyPulse/<version> (+repo URL)`.
- **Limits:** 100 requests per minute, at most 400 entries per response, 30 days of history.
  Stations are requested in batches of 20. 429 means rate limited; 204 means no data.
- **Format quirks handled in `skypulse.ingestion.metar.models`:**
  - `visib` is in statute miles, either a number or a string such as `"6+"` (unlimited),
    converted to metres with 9999 meaning 10 km or more.
  - `wdir` is `"VRB"` for variable wind.
  - `wxString` (present weather, e.g. `+TSRA`) is only present when there is weather.
- **Terms:** US government data (NOAA / NWS), public domain. Keep requests limited in scope
  and frequency; for bulk use the cache files instead.

## OurAirports (reference data, M5)

- **Download:** https://davidmegginson.github.io/ourairports-data/airports.csv
- **Content:** airports with ICAO/IATA codes, coordinates, country, type.
- **Terms:** public domain.
