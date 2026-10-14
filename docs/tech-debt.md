# Technical debt

Known shortcuts, kept visible on purpose. Each item says why it exists and what "done" means.

## TD-001: Remove hard-coded values with python-decouple

- **Raised:** 2026-10-14, during M2
- **When:** final hardening pass, after the platform is functional end to end (after M9)
- **Why it exists:** milestones prioritise a working pipeline; some defaults live in code.
- **Scope:** every literal that is really configuration, for example:
  - `LOCALSTACK_ENDPOINT`, Kafka bootstrap default, bucket prefix (`skypulse.common.config`)
  - aviationweather.gov User-Agent and API base URLs in the ingestion clients
  - retry policy defaults (`skypulse.common.http.RetryPolicy`)
  - topic names, polling intervals, airport lists and bounding boxes used by producers
  - credentials-related defaults in `docker-compose.yml` and CI
- **Approach:** read configuration through
  [python-decouple](https://github.com/HBNetwork/python-decouple) (`config()` with explicit
  casts and `.env`/`settings.ini` support). Decide at that point how it coexists with the
  typed `Settings` model (decouple as the source, Pydantic for validation) and record the
  choice in an ADR.
- **Done when:** a grep for URLs, hostnames, ports and magic numbers in `src/` finds only
  true constants (contract namespace, protocol values), and `.env.example` documents every
  setting.

## TD-002: Airline reference data is curated by hand

- **Raised:** 2026-10-14, during M5
- **Why it exists:** OurAirports has no airline data and open airline registries need
  licensing review. `src/skypulse/reference/airlines.json` lists the carriers in the
  fixtures.
- **Effect:** a carrier missing from the list has no country, so arrivals into the EU261
  area on that carrier are treated as not covered.
- **Done when:** airlines come from a maintained source loaded by `skypulse.batch reference`.

## TD-003: Replace synthetic schedules with real data

- **Raised:** 2026-10-14, during M5
- **Why it exists:** scheduled versus actual times come from AeroDataBox, which needs a paid
  key, so the published site and the demo run on a seeded synthetic schedule day. Aircraft
  positions and METAR reports are already real recordings.
- **Effect:** delays, claims and EU261 amounts on the site are illustrative, not measured.
- **Path to real data:**
  1. Subscribe to AeroDataBox (RapidAPI) and run the schedules feed live for the monitored
     airports; the `AeroDataBoxProvider` and parser already exist.
  2. Register an OpenSky account (OAuth2 client) and enable the observed-flights batch
     (`/flights/departure|arrival`), an independent actual-time source to cross-check the
     schedule provider.
  3. Record continuously (scheduled ingestion to the lake) instead of one replay day, so
     the gold layer accumulates weeks of history and the insights show trends.
  4. Publish the site from that history, keeping the fixture build for CI.
- **Done when:** the published insights are built from recorded real schedules over a
  rolling window, and the synthetic generator is used only by tests.
