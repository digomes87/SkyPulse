"""Weekly reload of airports, runways, countries and airlines into silver."""

from datetime import UTC, datetime

from airflow.sdk import DAG
from skypulse_tasks import DEFAULT_ARGS, batch_task

with DAG(
    dag_id="reference_data",
    description="OurAirports and airline reference data into silver",
    schedule="@weekly",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["skypulse", "reference"],
):
    batch_task("load_reference", "reference")
