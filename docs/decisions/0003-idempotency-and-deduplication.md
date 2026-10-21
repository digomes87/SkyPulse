# 0003. Idempotency and deduplication

- Status: Accepted
- Date: 2026-10-14

## Context

Duplicates enter SkyPulse in several ordinary ways:

- Pollers see the same observation in consecutive calls (a METAR stays in the "last 2 hours"
  window for many polls; an unchanged flight shows up on every FIDS poll).
- A producer retries after a timeout whose request actually succeeded.
- A streaming micro-batch fails after writing and is replayed from its checkpoint.
- Someone replays bronze to rebuild silver.

End-to-end exactly-once across HTTP APIs, Kafka/Kinesis and S3 is not available, so the
platform aims for **effectively once**: at-least-once delivery plus idempotent writes.

## Decision

Deduplication happens at three layers, each cheap and each sufficient on its own for the
cases it covers:

1. **Deterministic event ids at the producer.** `event_id` is a UUIDv5 of the event type and
   natural key (`skypulse.contracts.Event.create`). The same observation always gets the same
   id, whichever process or run produced it. For schedules the key includes a hash of the
   mutable fields, so an unchanged flight keeps its id and a status change gets a new one.
2. **Idempotent Kafka producer.** `enable.idempotence=true`, `acks=all`: broker-side retries
   within a producer session do not duplicate records.
3. **Streaming dedup plus idempotent sink in silver.**
   - `withWatermark(event_time).dropDuplicatesWithinWatermark(["event_id"])` removes repeats
     inside the watermark with bounded state.
   - Each micro-batch is reduced to one row per natural key, then `MERGE`d:
     insert-only for immutable facts (aircraft states, METAR) and
     "update only if newer" for evolving entities (schedules by `observed_at`, observed
     flights by `last_seen`). A replayed batch or a duplicate older than the watermark
     therefore changes nothing.

Natural keys:

| Silver table | MERGE key | Update rule |
|--------------|-----------|-------------|
| `aircraft_states` | `icao24`, `last_contact` | insert-only |
| `observed_flights` | `icao24`, `callsign`, `first_seen` | update when `last_seen` is newer |
| `flight_schedules` | `flight_number`, `scheduled_departure_utc` | update when `observed_at` is newer |
| `metar_observations` | `station_icao`, `observed_at`, `report_type` | insert-only |

Bronze is deliberately **not** deduplicated: it is the faithful record of what arrived,
including duplicates, which keeps replays and audits honest.

## Watermarks and late data

| Table | Event time | Watermark | Reasoning |
|-------|-----------|-----------|-----------|
| `aircraft_states` | `last_contact` | 10 minutes | Positions arrive within seconds; older ones are useless for live tracking |
| `observed_flights` | `first_seen` | 2 days | OpenSky publishes flights in a nightly batch |
| `flight_schedules` | `observed_at` | 1 hour | Each poll stamps the time it fetched the flight |
| `metar_observations` | `observed_at` | 3 hours | METAR polls look back 2 hours; corrections can lag |

An event later than the watermark is dropped by the streaming state and never reaches
silver, but it is in bronze. `tests/unit/streaming/test_watermarks.py` pins both sides:
inside the watermark it lands exactly once, beyond it it stays in bronze only.

Dropped rows are counted (`skypulse_stream_late_rows_dropped`) and recovered by
`python -m skypulse.batch backfill`, which `lake_to_gold` runs right after silver. It reads
the bronze rows ingested since its last successful cutoff (with a one-hour overlap), parses
them with the silver code, deduplicates on `event_id` and uses the same idempotent MERGE, so
silver ends up complete and a rerun changes nothing. The reconciliation quality stage then
checks that every valid bronze event of the last 24 hours has its natural key in silver.

## Consequences

- Correctness does not depend on the watermark: it only bounds memory. Duplicates beyond it
  are still absorbed by the MERGE.
- MERGE costs more than append. Silver tables are small (per flight, per station, per
  aircraft-second) and can be Z-ordered or clustered on their keys if it starts to matter.
- Changing a natural key is a breaking change and needs a contract version bump.
