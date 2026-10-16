"""Weekly retraining of the delay model with a promotion gate.

Rebuilds the point-in-time feature table, trains a candidate, registers it in MLflow and
promotes it to ``champion`` only if it beats the current champion on the same validation
flights. A rejected candidate stays registered (tagged with the reason) for inspection;
the prediction API keeps serving the champion and picks up a new one within minutes.
"""

from datetime import UTC, datetime

from airflow.sdk import DAG
from skypulse_tasks import DEFAULT_ARGS, ml_feature_task, ml_training_task

with DAG(
    dag_id="model_retraining",
    description="Features, training and gated promotion of the delay model",
    schedule="0 4 * * 1",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["skypulse", "ml"],
):
    features = ml_feature_task("build_features")
    training = ml_training_task("train_and_promote")

    features >> training
