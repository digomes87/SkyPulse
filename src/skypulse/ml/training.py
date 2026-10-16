"""Training and evaluation of the delay model.

- Split by time, never at random: the newest 20% of flights (by scheduled departure)
  validate, the rest train. Early stopping uses the last slice of the training period, so
  the validation set is untouched until the final evaluation.
- Every metric is reported next to a baseline that uses the route's own recent delay
  rate, so a model that only learns "this route is often late" shows no lift.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import lightgbm
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score

from skypulse.ml.feature_spec import CATEGORICAL_FEATURES, FEATURE_SET_VERSION, LABELS
from skypulse.ml.model import OUTPUT_COLUMNS, DelayModel, Preprocessor

VALIDATION_SHARE = 0.2
EARLY_STOPPING_SHARE = 0.1
BASELINE_SCORE = {"delayed_15": "route_delay_15_rate", "delayed_180": "route_mean_arrival_delay"}
PARAMETERS: Mapping[str, Any] = {
    "objective": "binary",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_data_in_leaf": 40,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.9,
    "bagging_freq": 1,
    "lambda_l2": 1.0,
    "verbose": -1,
    "seed": 1014,
    "deterministic": True,
}
NUM_BOOST_ROUND = 600
EARLY_STOPPING_ROUNDS = 40


@dataclass(frozen=True)
class Split:
    train: pd.DataFrame
    validation: pd.DataFrame

    @classmethod
    def by_time(cls, features: pd.DataFrame, share: float = VALIDATION_SHARE) -> "Split":
        labelled = features.dropna(subset=list(LABELS)).sort_values("scheduled_departure_utc")
        cutoff = labelled["scheduled_departure_utc"].quantile(1 - share)
        return cls(
            labelled[labelled["scheduled_departure_utc"] < cutoff],
            labelled[labelled["scheduled_departure_utc"] >= cutoff],
        )


def _safe_auc(labels: pd.Series, scores: pd.Series) -> float:
    if labels.nunique() < 2:
        return float("nan")
    return float(roc_auc_score(labels, scores))


def evaluate(labels: pd.Series, probabilities: pd.Series, baseline: pd.Series) -> dict[str, float]:
    clipped = probabilities.clip(1e-6, 1 - 1e-6)
    filled_baseline = baseline.fillna(baseline.mean() if baseline.notna().any() else 0.0)
    return {
        "roc_auc": _safe_auc(labels, probabilities),
        "pr_auc": float(average_precision_score(labels, probabilities)),
        "brier": float(brier_score_loss(labels, probabilities)),
        "log_loss": float(log_loss(labels, clipped, labels=[0, 1])),
        "positive_rate": float(labels.mean()),
        "baseline_roc_auc": _safe_auc(labels, filled_baseline),
        "baseline_pr_auc": float(average_precision_score(labels, filled_baseline)),
    }


def _fit(train: pd.DataFrame, label: str, preprocessor: Preprocessor) -> lightgbm.Booster:
    ordered = train.sort_values("scheduled_departure_utc")
    stop_at = int(len(ordered) * (1 - EARLY_STOPPING_SHARE))
    fit, stop = ordered.iloc[:stop_at], ordered.iloc[stop_at:]
    categorical = list(CATEGORICAL_FEATURES)
    training = lightgbm.Dataset(
        preprocessor.transform(fit), fit[label], categorical_feature=categorical
    )
    stopping = lightgbm.Dataset(preprocessor.transform(stop), stop[label], reference=training)
    return lightgbm.train(
        dict(PARAMETERS),
        training,
        num_boost_round=NUM_BOOST_ROUND,
        valid_sets=[stopping],
        callbacks=[lightgbm.early_stopping(EARLY_STOPPING_ROUNDS, verbose=False)],
    )


@dataclass(frozen=True)
class TrainingResult:
    model: DelayModel
    metrics: Mapping[str, float]
    split: Split

    def importances(self) -> dict[str, dict[str, float]]:
        return {
            label: dict(
                zip(
                    booster.feature_name(),
                    np.round(booster.feature_importance("gain"), 2).tolist(),
                    strict=True,
                )
            )
            for label, booster in self.model.boosters.items()
        }


def score(model: DelayModel, frame: pd.DataFrame) -> dict[str, float]:
    """Metrics of a model on labelled flights, for each label, prefixed with the label."""
    predictions = model.predict_frame(frame)
    metrics: dict[str, float] = {}
    for label in LABELS:
        result = evaluate(
            frame[label].astype(int),
            predictions[OUTPUT_COLUMNS[label]],
            frame[BASELINE_SCORE[label]],
        )
        metrics |= {f"{label}_{name}": value for name, value in result.items()}
    return metrics


def train(features: pd.DataFrame) -> TrainingResult:
    current = features[features["feature_set_version"] == FEATURE_SET_VERSION]
    split = Split.by_time(current)
    preprocessor = Preprocessor.fit(split.train)
    boosters = {label: _fit(split.train, label, preprocessor) for label in LABELS}
    model = DelayModel(preprocessor, boosters)
    metrics = score(model, split.validation) | {
        "train_rows": float(len(split.train)),
        "validation_rows": float(len(split.validation)),
    }
    return TrainingResult(model, metrics, split)
