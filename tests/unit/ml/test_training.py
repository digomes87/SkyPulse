"""Train on a few weeks of synthetic history and check the model learns the planted causes."""

from collections.abc import Iterator
from datetime import UTC, datetime

import pandas as pd
import pytest
from deltalake import DeltaTable
from pyspark.sql import SparkSession

from skypulse.common.config import Settings
from skypulse.ml.backfill import backfill
from skypulse.ml.features import build_feature_tables, features_path
from skypulse.ml.history import SyntheticHistory
from skypulse.ml.registry import CHAMPION, decide, log_and_register, promote
from skypulse.ml.training import Split, train

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def environment(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Settings]:
    root = tmp_path_factory.mktemp("ml")
    patch = pytest.MonkeyPatch()
    patch.setenv("SKYPULSE_LAKE__ROOT_URI", root.as_uri())
    patch.setenv("SKYPULSE_ML__TRACKING_URI", f"sqlite:///{root / 'mlflow.db'}")
    yield Settings()
    patch.undo()


@pytest.fixture(scope="module")
def features(spark: SparkSession, environment: Settings) -> pd.DataFrame:
    backfill(spark, environment, SyntheticHistory(datetime(2026, 8, 1, tzinfo=UTC), days=35))
    build_feature_tables(spark, environment)
    return DeltaTable(features_path(environment)).to_pandas()


def test_split_is_by_time(features: pd.DataFrame) -> None:
    split = Split.by_time(features)

    assert (
        split.train["scheduled_departure_utc"].max()
        < split.validation["scheduled_departure_utc"].min()
    )
    assert len(split.validation) == pytest.approx(
        0.2 * (len(split.train) + len(split.validation)), rel=0.05
    )


def test_model_beats_the_route_baseline(features: pd.DataFrame) -> None:
    metrics = train(features).metrics

    assert metrics["delayed_15_roc_auc"] > metrics["delayed_15_baseline_roc_auc"] + 0.05
    assert metrics["delayed_15_roc_auc"] > 0.7
    assert 0 < metrics["delayed_180_positive_rate"] < 0.2


def test_weather_matters_to_the_model(features: pd.DataFrame) -> None:
    importances = train(features).importances()["delayed_15"]
    ranked = sorted(importances, key=importances.__getitem__, reverse=True)

    assert {"departure_hour_utc", "origin_thunderstorm", "origin_visibility_m"} & set(ranked[:6])


def test_first_model_becomes_champion_and_an_identical_one_does_not(
    features: pd.DataFrame, environment: Settings
) -> None:
    result = train(features)

    first = log_and_register(result, environment, data_version=0)
    second = log_and_register(result, environment, data_version=0)

    assert promote(environment, first, result.split.validation).promoted
    rejected = promote(environment, second, result.split.validation)
    assert not rejected.promoted
    assert "below" in rejected.reason
    from mlflow.tracking import MlflowClient  # noqa: PLC0415 - after the tracking URI is set

    champion = MlflowClient(environment.ml.tracking_uri).get_model_version_by_alias(
        environment.ml.model_name, CHAMPION
    )
    assert str(champion.version) == first


def test_decision_guards_the_rare_label() -> None:
    champion = {"delayed_15_pr_auc": 0.50, "delayed_180_pr_auc": 0.30}

    assert decide({"delayed_15_pr_auc": 0.52, "delayed_180_pr_auc": 0.30}, champion).promoted
    assert not decide({"delayed_15_pr_auc": 0.52, "delayed_180_pr_auc": 0.25}, champion).promoted
    assert decide({"delayed_15_pr_auc": 0.40, "delayed_180_pr_auc": 0.10}, None).promoted
