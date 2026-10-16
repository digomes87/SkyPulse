"""Model tracking, registration and the promotion gate (MLflow).

A new model is registered as a version of ``skypulse-delay``. It becomes ``champion`` only
if, on the same validation flights, it beats the current champion's PR-AUC for 15-minute
delays by a margin and does not lose more than a tolerance on 180-minute delays. Both
models are scored on today's validation set, so an old champion is never compared on the
numbers it happened to get on older data.
"""

import math
from collections.abc import Mapping
from dataclasses import dataclass

import mlflow
import pandas as pd
from mlflow.exceptions import MlflowException
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient

from skypulse.common.config import Settings
from skypulse.ml.feature_spec import FEATURE_SET_VERSION, FEATURES
from skypulse.ml.model import DelayModel
from skypulse.ml.training import PARAMETERS, TrainingResult, score

CHAMPION = "champion"
PRIMARY_METRIC = "delayed_15_pr_auc"
GUARD_METRIC = "delayed_180_pr_auc"
MIN_IMPROVEMENT = 0.002
GUARD_TOLERANCE = 0.01


def _finite(metrics: Mapping[str, float]) -> dict[str, float]:
    return {name: value for name, value in metrics.items() if not math.isnan(value)}


def log_and_register(result: TrainingResult, settings: Settings, data_version: int | None) -> str:
    mlflow.set_tracking_uri(settings.ml.tracking_uri)
    mlflow.set_experiment(settings.ml.experiment)
    example = result.split.validation[list(FEATURES)].head(5)
    with mlflow.start_run() as run:
        mlflow.log_params({**PARAMETERS, "feature_set_version": FEATURE_SET_VERSION})
        mlflow.log_metrics(_finite(result.metrics))
        mlflow.log_dict(result.importances(), "feature_importance.json")
        mlflow.set_tags({"feature_table_version": str(data_version), "model_type": "lightgbm"})
        info = mlflow.pyfunc.log_model(
            name="model",
            python_model=result.model,
            registered_model_name=settings.ml.model_name,
            input_example=example,
            signature=infer_signature(example, result.model.predict_frame(example)),
        )
        mlflow.set_tag("run_id", run.info.run_id)
    return str(info.registered_model_version)


def load_version(settings: Settings, version: str) -> DelayModel:
    loaded = mlflow.pyfunc.load_model(f"models:/{settings.ml.model_name}/{version}")
    model: DelayModel = loaded.unwrap_python_model()
    return model


@dataclass(frozen=True)
class Decision:
    promoted: bool
    candidate: Mapping[str, float]
    champion: Mapping[str, float] | None
    reason: str


def decide(candidate: Mapping[str, float], champion: Mapping[str, float] | None) -> Decision:
    if champion is None:
        return Decision(True, candidate, None, "no champion yet")
    gain = candidate[PRIMARY_METRIC] - champion[PRIMARY_METRIC]
    guard_loss = champion[GUARD_METRIC] - candidate[GUARD_METRIC]
    if gain < MIN_IMPROVEMENT:
        return Decision(
            False, candidate, champion, f"{PRIMARY_METRIC} gain {gain:+.4f} below {MIN_IMPROVEMENT}"
        )
    if guard_loss > GUARD_TOLERANCE:
        return Decision(False, candidate, champion, f"{GUARD_METRIC} drops by {guard_loss:.4f}")
    return Decision(True, candidate, champion, f"{PRIMARY_METRIC} gain {gain:+.4f}")


def promote(settings: Settings, version: str, validation: pd.DataFrame) -> Decision:
    client = MlflowClient(settings.ml.tracking_uri)
    name = settings.ml.model_name
    candidate = score(load_version(settings, version), validation)
    try:
        current = client.get_model_version_by_alias(name, CHAMPION)
    except MlflowException:
        current = None
    champion = (
        None if current is None else score(load_version(settings, current.version), validation)
    )
    decision = decide(candidate, champion)
    if decision.promoted:
        client.set_registered_model_alias(name, CHAMPION, version)
    client.set_model_version_tag(
        name,
        version,
        "promotion",
        ("promoted: " if decision.promoted else "rejected: ") + decision.reason,
    )
    return decision
