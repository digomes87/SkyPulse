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
| `logging` | JSON logs to stdout with level, component, UTC timestamp and correlation id |
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
| `skypulse.raw.observed-flights` | `icao24` | 3 | 7 days |
| `skypulse.raw.flight-schedules` | flight number + date | 3 | 7 days |
| `skypulse.raw.metar` | airport ICAO | 3 | 7 days |
| `skypulse.events.flight-disruptions` | flight id | 3 | 7 days |
| `skypulse.dlq.raw` | original key | 1 | 14 days |

Keys keep every update for one aircraft, flight or airport in order on one partition.

## Lakehouse buckets

One bucket per layer, named `<prefix>-<layer>` (default prefix `skypulse`): `bronze`, `silver`,
`gold` and `checkpoints` (streaming state, kept apart so it can have its own lifecycle rules).

## Ingestion (`skypulse.ingestion`)

```
OpenSkyClient ─┐                       ┌─> skypulse.raw.<feed>
ScheduleProvider ├─> Poller ─> EventPublisher ─┤
AviationWeatherClient ┘   (contract check)      └─> skypulse.dlq.raw (violations)
```

- **Pollers** (`pollers.py`) run one cycle per source: fetch, build events, publish. The
  runner repeats them on an interval, each cycle under its own correlation id; a failed cycle
  is logged and skipped, and running out of OpenSky credits backs off for 15 minutes.
- **Events** carry a deterministic `event_id` (UUIDv5 of the natural key, see
  [contracts/README.md](../contracts/README.md)), so the same observation ingested twice is
  the same event downstream.
- **Replay mode** (`SKYPULSE_INGESTION__REPLAY=true`) swaps the HTTP transport for one that
  answers from `data/fixtures/` and the schedule provider for `ReplayProvider`. A replay clock
  (default start 2026-10-14T05:00Z, 60x speed) walks through the recorded snapshots. Clients,
  parsers and the publisher are the production code paths.
- **Wiring** (`wiring.py`) is the only module that picks concrete implementations.

## Streaming lakehouse (`skypulse.streaming`)

```
Kafka skypulse.(raw|dlq).*
   │  bronze_raw_events (append, partitioned by topic, ingest_date)
   ▼
bronze/raw_events ──┬─ silver_<table>     parse → required checks → watermark dedup → MERGE
                    └─ quarantine_<table> parse failures → silver/quarantine (append)
```

| Module | Responsibility |
|--------|----------------|
| `schemas` | Spark `StructType`s generated from the JSON Schema contracts |
| `bronze` | Kafka record → bronze row (payload as string, headers extracted), Kafka streaming query |
| `silver` | Table specs (event time, watermark, natural key, update rule), parsing, quarantine, dedup, MERGE |
| `jobs` | Streaming queries bronze → silver and bronze → quarantine |
| `session` | SparkSession with Delta and only the connectors a stage needs |
| `summary` | Row counts per table, used by `make demo` |

Run with `python -m skypulse.streaming bronze|silver [--available-now]`. In containers the
`spark` image already holds the Delta, Kafka and S3A packages. Design choices are recorded
in [ADR 0002](decisions/0002-delta-lake-medallion.md) (Delta, medallion) and
[ADR 0003](decisions/0003-idempotency-and-deduplication.md) (idempotency, watermarks).

## Disruptions and EU261 claims

```
silver/flight_schedules ──change data feed──> detector (Spark)
     classify -> enrich (airports, airlines, METAR ±1 h) -> drop already announced
     -> validate contract -> SNS flight-disruptions -> record in silver/flight_disruptions
                                   │
         ┌─────────────────────────┴──────────────────────────┐
  SQS eu261-calculator (filter: kind in              SQS notifications
  arrival_delay, cancellation)                              │
         │  DLQ after 3 receives                             │  DLQ after 3 receives
  Lambda eu261-calculator                            Lambda notifier
   skypulse.eu261.assess -> DynamoDB eu261-claims     JSON log + optional webhook
   (conditional on detected_at)
```

| Piece | Module |
|-------|--------|
| Rules | `skypulse.eu261` (pure, stdlib only; [ADR 0005](decisions/0005-eu261-rules-engine.md)) |
| Detector | `skypulse.streaming.disruptions`, `detector`, `announcements` |
| Lambdas | `skypulse.lambdas.eu261_calculator`, `notifier` |
| Packaging and local deploy | `skypulse.deployment.lambdas` (`make lambdas`) |
| Reference data | `skypulse.reference` (curated now, OurAirports in M5) |
| Operations | [runbooks/sqs-dead-letter-redrive.md](runbooks/sqs-dead-letter-redrive.md) |

Delivery is at-least-once end to end. Idempotency comes from the detector's record of
announced `(flight, scheduled departure, kind)` and the calculator's conditional write:
a claim is only replaced by a version detected later.

## Batch jobs (`skypulse.batch`)

`python -m skypulse.batch reference|assessments|maintenance`, scheduled by Airflow.

| Job | Reads | Writes | Notes |
|-----|-------|--------|-------|
| `reference` | OurAirports CSVs (fixtures in replay mode), curated airlines | `silver/reference_airports` (with runway headings), `reference_countries`, `reference_airlines` | Overwrite; the detector uses the runway headings for crosswind |
| `assessments` | `silver/flight_disruptions` | `silver/eu261_assessments` | Maps the pure `skypulse.eu261` function over every disruption; a full recompute, so rule changes reach history |
| `maintenance` | silver and bronze tables | same | OPTIMIZE with Z-order on query keys, VACUUM 7 days |

## Gold layer (`dbt/`)

dbt on DuckDB reads the silver Delta tables in place (`delta_scan`) for local runs and CI;
cross-database macros keep the SQL portable to Databricks. Staging views, dimensions
(`dim_airport`, `dim_airline`, `dim_route`, `dim_date`), facts (`fct_flight`,
`fct_eu261_claim`) and aggregates (`agg_airport_daily_delays`, `agg_route_otp`,
`agg_eu261_exposure_by_airline`), with 58 tests. `make lake && make dbt-build` builds it
locally; CI does the same in the `gold` job.

## Orchestration (`airflow/`)

Airflow 3 (LocalExecutor, Postgres metadata) under the compose `orchestration` profile, UI
on http://localhost:8088. Airflow only orchestrates: every task is a container from the
SkyPulse images, started by the DockerOperator through a Docker socket proxy that exposes
nothing but containers, images and networks.

| DAG | Schedule | Tasks |
|-----|----------|-------|
| `reference_data` | weekly | `skypulse.batch reference` |
| `metar_refresh` | hourly | one METAR poll for every monitored airport |
| `lake_to_gold` | hourly | bronze → silver → detector → EU261 assessments → `dbt build` |
| `lake_maintenance` | daily 03:00 UTC | OPTIMIZE + Z-order, VACUUM 7 days |

Every task is idempotent (checkpoints, MERGE on natural keys, full recompute of
assessments), so retries and reruns are safe; a failing dbt test fails the run. In AWS the
same steps map to EMR Serverless and ECS tasks (M8). `make airflow-validate` checks the DAG
folder inside the real Airflow image in CI.

## Data quality (`skypulse.quality`)

| Stage | Runs after | What | Effect of a failure |
|-------|-----------|------|---------------------|
| `silver` | silver streaming | Great Expectations suites: formats, ranges, natural-key uniqueness | **Gate**: Airflow stops the detector and everything after it |
| `gold` | `dbt build` | Suites on business distributions (cancellation share, median delay, claim amounts) | **Gate** for gold consumers |
| `statistics` | `dbt build` | Freshness SLAs, hourly volume anomaly (seasonal baseline, Poisson-floored z-score), null-rate drift against the previous run | **Monitor**: metrics and alerts, never blocks |

dbt tests guard structure (keys, relationships, accepted values); the suites guard
plausibility. Every run is appended to `silver/quality_results` and pushed to Prometheus.

## Observability (`skypulse.observability`, `observability/`)

| Source | How | Metrics |
|--------|-----|---------|
| Producers | scrape `:9464` | events published and dead-lettered, poll duration and failures, HTTP retries, OpenSky credits |
| Streaming jobs | `StreamingQueryListener`, scrape `:9466` or push | rows and rates, batch duration, watermark delay, offsets behind Kafka (lag) |
| Batch and quality runs | Pushgateway | the same, plus check status and failed-check count |
| Exporter | scrape `:9465` | last commit per Delta table (delta-rs, no Spark), freshness SLA breaches, SQS DLQ depth, Kafka DLQ topic size, claims stored |

Grafana (`make observability`, http://localhost:3000) provisions two dashboards and four
alert rules generated by `skypulse.observability.dashboards`; a unit test fails if the
committed JSON drifts from the generator or queries a metric nothing exports. Each alert
links its runbook in [runbooks/](runbooks/).
