# Architecture

This document describes how SkyPulse is put together and is updated at the end of every
milestone. The README has the high-level diagram; decisions are recorded in
[decisions/](decisions/).

## Run modes

| Concern | `local` | `aws` |
|---------|---------|-------|
| Event bus | Kafka 4.1 (KRaft, single node) | Kinesis Data Streams |
| Object storage | LocalStack S3 | S3 |
| Messaging | LocalStack SNS / SQS | SNS / SQS |
| Functions and state | LocalStack Lambda / DynamoDB | Lambda / DynamoDB |
| Provisioning | `docker-compose.yml`, `infra/local/` | `infra/terraform/` (M8) |

The mode is chosen with `SKYPULSE_RUN_MODE`. All settings are read by
`skypulse.common.config.Settings` from `SKYPULSE_*` environment variables or `.env`.

## Shared building blocks (`skypulse.common`)

| Module | Responsibility |
|--------|----------------|
| `config` | Typed, immutable settings; lake bucket naming per layer |
| `correlation` | `CorrelationId` value type and a context-scoped current id |
| `logging` | JSON logs to stdout with level, logger, UTC timestamp and correlation id |
| `streams` | `Topic`, `PartitionKey`, `StreamRecord`, the `StreamSink` / `StreamSource` protocols and an in-memory implementation |

### Correlation ids

A correlation id is created where an event enters the system (the producer), written into the
`correlation-id` header of the stream record, and re-bound by every downstream consumer. Any
log line written inside `correlation_scope(...)` carries it, which makes it possible to follow a
single flight update from the API call through the stream job to the Lambda that stores a claim.

## Topics

Declared in `infra/local/kafka/topics.txt`:

| Topic | Key | Partitions | Retention |
|-------|-----|-----------|-----------|
| `skypulse.raw.aircraft-states` | `icao24` | 6 | 1 day |
| `skypulse.raw.flight-schedules` | flight number + date | 3 | 7 days |
| `skypulse.raw.metar` | airport ICAO | 3 | 7 days |
| `skypulse.events.flight-disruptions` | flight id | 3 | 7 days |
| `skypulse.dlq.raw` | original key | 1 | 14 days |

Keys keep every update for one aircraft, flight or airport in order on one partition.

## Lakehouse buckets

One bucket per layer, named `<prefix>-<layer>` (default prefix `skypulse`): `bronze`, `silver`,
`gold` and `checkpoints` (streaming state, kept apart so it can have its own lifecycle rules).
