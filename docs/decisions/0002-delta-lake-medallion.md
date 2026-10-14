# 0002. Delta Lake and a medallion layout

- Status: Accepted
- Date: 2026-10-14

## Context

SkyPulse needs a lake that a streaming job can write to continuously while batch jobs, dbt
and ad hoc queries read from it. It needs ACID appends from streaming, upserts on natural
keys, time travel for debugging, and compaction. It should run on S3 in AWS, on
Databricks, and on a laptop.

The two realistic table formats are Delta Lake and Apache Iceberg.

## Decision

**Delta Lake 4.0 on Spark 4.0**, organised as bronze, silver and gold:

| Layer | Content | Write pattern |
|-------|---------|---------------|
| Bronze `raw_events` | Every Kafka record from `skypulse.raw.*` and `skypulse.dlq.raw`; payload kept as a string, plus topic, partition, offset, correlation id and DLQ reason | Streaming append, partitioned by `topic`, `ingest_date` |
| Silver `aircraft_states`, `observed_flights`, `flight_schedules`, `metar_observations` | Parsed with the contract schema, typed, deduplicated | Streaming `foreachBatch` MERGE on the natural key |
| Silver `quarantine` | Rows that failed parsing or required-field checks, with the reason | Streaming append |
| Gold | dbt models (M5) | Batch |

Why Delta over Iceberg here:

- **Databricks is a target.** Delta is native there (Unity Catalog, OPTIMIZE, Z-order,
  liquid clustering) and SkyPulse's jobs should run unchanged on Databricks.
- **Streaming MERGE is mature.** `foreachBatch` + `DeltaTable.merge` is the documented
  pattern for idempotent upserts from Structured Streaming.
- **One dependency.** `delta-spark` from PyPI configures the session itself; local
  development needs no catalog service.
- Iceberg would be the pick for a multi-engine lake (Trino, Flink, Snowflake) with a REST
  catalog. Delta UniForm can expose Iceberg metadata later if that becomes a requirement.

Why a single bronze table instead of one per topic: one Kafka query, one checkpoint and
one place to look for "did we receive it". Partitioning by topic keeps per-topic reads
cheap.

Why silver schemas come from the contracts (`skypulse.streaming.schemas`): one source of
truth. A field added to a contract shows up in silver without touching the job.

## Consequences

- Spark 4 and Java 17+ are required for streaming jobs; producers stay JVM-free.
- Silver tables are created on the first micro-batch from the batch schema. Schema
  evolution of silver follows the contract rules (see `contracts/README.md`); additive
  changes need `mergeSchema` on the MERGE when they happen.
- Many small files are expected from frequent micro-batches. OPTIMIZE and VACUUM run as
  Airflow maintenance jobs (M5), with retention long enough for time-travel debugging.
- In local mode tables live in LocalStack S3 (`s3a://skypulse-<layer>`) or, for tests,
  under `SKYPULSE_LAKE__ROOT_URI`.
