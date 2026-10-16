# Runbook: table freshness SLA breached

**Alert:** `Table freshness SLA breached` (Grafana, after 10 minutes)
**Signal:** `skypulse_table_freshness_sla_breached{table=...} == 1`, from the exporter, which
reads each Delta table's last commit time.
**Severity:** medium. Consumers see old data; nothing is lost while Kafka retains the events.

| Table | SLA |
|-------|-----|
| `aircraft_states` | 15 min |
| `flight_schedules` | 30 min |
| `metar_observations` | 90 min |
| `flight_disruptions`, `eu261_assessments` | 2 h |
| `reference_airports` | 8 days |

## 1. Find where the flow stopped

Walk the pipeline from the source:

1. **Producer running?** Pipeline health dashboard → *Events published per second* for the
   feed, *Failed polls*. Logs: `docker compose logs producer-<feed>`; look for
   `poll_failed`, `poll_out_of_credits`.
2. **Events in Kafka?** `kafka-console-consumer --topic skypulse.raw.<feed> --max-messages 1`.
3. **Bronze moving?** *Minutes since last change* for `bronze.raw_events`.
4. **Silver moving?** In Airflow, the last `lake_to_gold` run and the `silver` task log.

## 2. Usual causes

| Cause | Fix |
|-------|-----|
| OpenSky credits exhausted (`skypulse_opensky_credits_remaining` at 0) | Wait for the daily reset; the producer backs off on its own. Reduce the bounding box if it recurs. |
| Upstream API down (HTTP retries climbing) | Nothing to fix locally; confirm on the provider's status page and let retries catch up. |
| Airflow run failed (quality gate or task error) | Open the run, read the failing task; see the quality-gate runbook. |
| Streaming query died | Restart the stage; checkpoints resume where it stopped, no data is lost. |

## 3. Recover

Once the cause is fixed, trigger `lake_to_gold` manually. Every step is idempotent, so a
rerun simply catches up. The gauge returns to 0 within one exporter interval (30 s).
