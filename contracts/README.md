# Event contracts

JSON Schema (draft 2020-12) definitions for every event SkyPulse puts on the stream.
They are the single source of truth for producers (validation before publish), the
bronze-to-silver job (validation and typing) and the documentation.

## Envelope

Every event shares the same envelope:

| Field | Meaning |
|-------|---------|
| `event_id` | UUIDv5 derived from the natural key of the observation. Re-ingesting the same observation produces the same id, which makes downstream deduplication trivial. |
| `event_type` | `aircraft_state`, `observed_flight`, `flight_schedule`, `metar_observation`, `flight_disruption` |
| `schema_version` | Integer, bumped only on breaking changes |
| `source` | System the data came from (`replay` when served from fixtures) |
| `produced_at` | UTC timestamp at which the producer emitted the event |
| `data` | Event payload, defined per contract |

## Natural keys

| Event | Natural key |
|-------|-------------|
| `aircraft_state` | `icao24` + `last_contact` |
| `observed_flight` | `icao24` + `first_seen` |
| `flight_schedule` | `flight_number` + `scheduled_departure_utc` + hash of the mutable fields (status, revised and runway times). Polling an unchanged flight yields the same id; any change yields a new event. |
| `metar_observation` | `station_icao` + `observed_at` + `report_type` |
| `flight_disruption` | `flight_number` + `scheduled_departure_utc` + `kind`: each kind of disruption is announced once per flight |

## Evolution rules

- Producers are strict: `additionalProperties: false`, so an unexpected field fails fast
  and goes to the dead-letter topic instead of silently polluting bronze.
- **Backward-compatible change** (new optional/nullable field, widened enum): edit the
  schema in place, keep `schema_version`, add the field to the silver schema with a
  default. Old events stay readable.
- **Breaking change** (removed or renamed field, changed type or meaning): new file
  `<name>.v2.json`, `schema_version: 2`. Producers dual-write during the migration and the
  bronze-to-silver job reads both versions until v1 traffic stops.
- Bronze stores the raw JSON as-is (schema-on-read), so no historical data is lost when a
  contract changes.
