# Runbook: streaming is falling behind Kafka

**Alert:** `Streaming is falling behind Kafka` (lag above 10,000 offsets for 15 minutes)
**Signal:** `skypulse_stream_offsets_behind_latest{query=...}`, reported by Spark's Kafka
source each micro-batch (Spark keeps offsets in its checkpoint, not a consumer group).

## 1. Is it growing or steady?

Pipeline health dashboard → *Kafka lag*, *Rows processed per second*, *Micro-batch
duration*.

- **Lag growing, batch duration rising:** the job can't keep up.
- **Lag steady, no batches:** the query stopped (check `skypulse_stream_terminations_total`
  and the job logs).

## 2. Fixes

| Cause | Fix |
|-------|-----|
| Burst after an outage | Let it drain; `maxOffsetsPerTrigger` keeps batches bounded |
| Slow MERGE in silver | Run `lake_maintenance` (OPTIMIZE + Z-order) and check table file counts |
| Driver memory pressure | Raise `SKYPULSE_SPARK__DRIVER_MEMORY` |
| Query died | Restart the stage; it resumes from its checkpoint |

Retention on the raw topics is 1 to 7 days (see `infra/local/kafka/topics.txt`); if lag
approaches retention, events will be lost from Kafka. Bronze then has a gap that a replay
from the source (or fixtures) must fill.
