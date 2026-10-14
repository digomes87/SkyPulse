"""Daily Delta maintenance: OPTIMIZE with Z-ordering, then VACUUM (7-day retention)."""

from datetime import UTC, datetime

from airflow.sdk import DAG
from skypulse_tasks import DEFAULT_ARGS, batch_task

with DAG(
    dag_id="lake_maintenance",
    description="Compact small files and remove unreferenced ones",
    schedule="0 3 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["skypulse", "maintenance"],
):
    batch_task("optimize_and_vacuum", "maintenance")
