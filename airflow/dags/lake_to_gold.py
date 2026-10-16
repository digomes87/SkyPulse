"""Hourly lakehouse refresh: drain Kafka into bronze and silver, check silver, detect
disruptions, recompute EU261 assessments, rebuild the dbt gold layer, then check gold.

Every step is idempotent (Delta checkpoints, MERGE on natural keys, full recompute of
assessments), so a retry or a manual rerun never double-counts. The silver expectation
suite gates everything built on silver; dbt tests and the gold suite gate the gold
consumers. The statistics stage (freshness, volume, null drift) reports without gating.
"""

from datetime import UTC, datetime

from airflow.sdk import DAG
from skypulse_tasks import DEFAULT_ARGS, batch_task, dbt_task, quality_task, spark_task

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
    silver_quality = quality_task("quality_silver", "silver")
    gold_quality = quality_task("quality_gold", "gold")
    statistics = quality_task("quality_statistics", "statistics")

    bronze >> silver >> silver_quality >> detector >> assessments >> gold
    gold >> [gold_quality, statistics]
