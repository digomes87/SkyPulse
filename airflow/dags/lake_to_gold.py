"""Hourly lakehouse refresh: drain Kafka into bronze and silver, detect disruptions,
recompute EU261 assessments and rebuild the dbt gold layer with its tests.

Every step is idempotent (Delta checkpoints, MERGE on natural keys, full recompute of
assessments), so a retry or a manual rerun never double-counts. dbt tests fail the run
and block the downstream consumers of gold.
"""

from datetime import UTC, datetime

from airflow.sdk import DAG
from skypulse_tasks import DEFAULT_ARGS, batch_task, dbt_task, spark_task

with DAG(
    dag_id="lake_to_gold",
    description="Bronze, silver, disruption detection, EU261 assessments and dbt build",
    schedule="@hourly",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["skypulse", "lakehouse", "dbt"],
):
    bronze = spark_task("bronze", "bronze", "--available-now")
    silver = spark_task("silver", "silver", "--available-now")
    detector = spark_task("detect_disruptions", "detector", "--available-now")
    assessments = batch_task("recompute_assessments", "assessments")
    gold = dbt_task("dbt_build", "build")

    bronze >> silver >> detector >> assessments >> gold
