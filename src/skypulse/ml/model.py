"""The delay model as one MLflow pyfunc: preprocessing plus a booster per label.

Packaging the category mappings with the boosters means serving cannot drift from
training: the same object turns raw features into model inputs in both places.
"""

from collections.abc import Mapping
from typing import Any

import lightgbm
import pandas as pd
from mlflow.pyfunc.model import PythonModel

from skypulse.ml.feature_spec import CATEGORICAL_FEATURES, FEATURES, LABELS

OUTPUT_COLUMNS = {label: f"probability_{label}" for label in LABELS}


class Preprocessor:
    """Fixed categories learned at training time; unseen values become missing."""

    def __init__(self, categories: Mapping[str, list[str]]) -> None:
        self.categories = {column: list(values) for column, values in categories.items()}

    @classmethod
    def fit(cls, frame: pd.DataFrame) -> "Preprocessor":
        return cls(
            {column: sorted(frame[column].dropna().unique()) for column in CATEGORICAL_FEATURES}
        )

    def transform(self, frame: pd.DataFrame) -> pd.DataFrame:
        prepared = frame.reindex(columns=list(FEATURES)).copy()
        for column, values in self.categories.items():
            prepared[column] = pd.Categorical(prepared[column], categories=values)
        numeric = [column for column in FEATURES if column not in self.categories]
        prepared[numeric] = prepared[numeric].astype("float64")
        return prepared


class DelayModel(PythonModel):
    def __init__(
        self, preprocessor: Preprocessor, boosters: Mapping[str, lightgbm.Booster]
    ) -> None:
        self.preprocessor = preprocessor
        self.boosters = dict(boosters)

    def predict_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        prepared = self.preprocessor.transform(frame)
        return pd.DataFrame(
            {
                OUTPUT_COLUMNS[label]: booster.predict(prepared)
                for label, booster in self.boosters.items()
            },
            index=frame.index,
        )

    def predict(  # MLflow calls this with context and params it does not need here
        self,
        context: Any,  # noqa: ARG002
        model_input: pd.DataFrame,
        params: dict[str, Any] | None = None,  # noqa: ARG002
    ) -> pd.DataFrame:
        return self.predict_frame(model_input)
