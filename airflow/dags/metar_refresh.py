"""Hourly METAR poll for every monitored airport (fills gaps the streaming feed missed)."""

from datetime import UTC, datetime

from airflow.sdk import DAG
from skypulse_tasks import DEFAULT_ARGS, producer_task

with DAG(
    dag_id="metar_refresh",
    description="Poll aviationweather.gov for the last two hours of METAR",
    schedule="@hourly",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["skypulse", "ingestion"],
):
    producer_task("poll_metar", "metar")
