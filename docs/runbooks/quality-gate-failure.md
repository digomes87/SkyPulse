# Runbook: quality gate failure

**Alert:** `Quality checks failing` (fires immediately)
**Signal:** `skypulse_quality_checks_failed > 0`, pushed by `python -m skypulse.quality`.
**Effect:** the Airflow task `quality_silver` or `quality_gold` failed, so downstream tasks
did not run. Gold still shows the previous good build; nothing wrong was published.

## 1. Which checks failed

- Data quality dashboard → *Failing quality checks* lists suite and check.
- Every run is kept in `silver/quality_results` with the observed value and details:

  ```sql
  -- DuckDB, local lake
  select run_at, suite, "check", observed, details
  from delta_scan('data/lake/silver/quality_results')
  where status = 'fail'
  order by run_at desc limit 20;
  ```

- The Airflow task log has one `quality_check_failed` line per failure.

## 2. Decide: data or rule?

| Failure | Likely meaning | Action |
|---------|----------------|--------|
| `silver.*` regex / set / range | A source changed its format | Check `silver/quarantine` and the producer DLQ; fix the parser or contract, then backfill from bronze |
| `silver.*` compound uniqueness | Dedup or MERGE broken | Stop and investigate the streaming job before anything else |
| `gold.fct_flight` cancellation share or median delay | Either a real disruption day or a feed problem | Compare with raw counts in bronze; if real, record it and widen the bound deliberately |
| `gold.fct_eu261_claim` amounts | Rules engine or assessment bug | Reproduce with `skypulse.eu261.assess` on the claim's disruption payload |

Never relax a bound just to make the alert go away: change it in
`src/skypulse/quality/suites.py` with a commit message explaining why.

## 3. Recover

Fix the cause, then clear the failed task in Airflow (or trigger `lake_to_gold`). The gate
reruns and, once green, the downstream tasks run.
