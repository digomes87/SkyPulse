# 0001. Streaming abstraction over Kafka and Kinesis

- Status: Accepted
- Date: 2026-10-14

## Context

SkyPulse runs on Kafka locally and on Kinesis Data Streams in AWS (ADR 0004). The producers
should not care which one is underneath, and switching must not mean touching every
producer. The two systems differ a lot: partitions vs shards, offsets vs sequence numbers,
consumer groups vs iterators and enhanced fan-out, message headers (Kafka) vs none
(Kinesis), and different batching and size limits.

## Decision

1. Producers depend on two small protocols in `skypulse.common.streams`:
   `StreamSink.publish(topic, record)` / `flush()` and `StreamSource.consume(topic)`.
2. A `StreamRecord` is a partition key, an opaque byte payload and string headers. Event
   semantics (type, schema, ids) live in the payload envelope, not in the transport.
3. `publish` may buffer; `flush` is the durability point. A polling cycle counts as
   published only when its flush returns, and `flush` raises if any delivery failed.
4. Names are validated against the stricter of the two systems (Kinesis: 128 characters of
   `[A-Za-z0-9._-]`).
5. Keys are domain keys (`icao24`, flight number, station), so ordering per aircraft, flight
   or airport holds on both systems.
6. The Kafka sink uses an idempotent producer (`enable.idempotence`, `acks=all`). The Kinesis
   sink (M8) will carry headers inside the payload envelope because Kinesis has none.
7. Spark reads both through its native connectors, not through this interface. The
   abstraction is for Python producers and consumers only.

## Consequences

- Swapping transport is a wiring change (`skypulse.ingestion.wiring`), covered by unit tests
  that run against `InMemoryStream`.
- The interface is the common denominator: no transactions, no exactly-once producer
  semantics across sessions, no consumer offsets in the contract. Duplicates are expected
  and handled downstream by deterministic `event_id`s (ADR 0003).
- Transport-specific tuning (linger, compression, shard count) stays in the adapters and
  their configuration.
