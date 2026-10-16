"""Training entrypoint (ml image, no Spark): ``python -m skypulse.ml.train``.

Reads the feature table from the lake with delta-rs, trains, logs and registers the model
in MLflow, then runs the promotion gate.
"""

import pandas as pd
from deltalake import DeltaTable

from skypulse.common.config import LakeLayer, Settings
from skypulse.common.logging import configure_logging, get_logger
from skypulse.ml.registry import log_and_register, promote
from skypulse.ml.training import train
from skypulse.observability.lake import storage_options

_log = get_logger(__name__)


def load_features(settings: Settings) -> tuple[pd.DataFrame, int]:
    uri = f"{settings.lake.uri_for(LakeLayer.GOLD)}/flight_features".replace("s3a://", "s3://", 1)
    table = DeltaTable(uri, storage_options=storage_options(settings))
    return table.to_pandas(), table.version()


def main() -> int:
    settings = Settings()
    configure_logging(settings.log_level)
    features, data_version = load_features(settings)
    result = train(features)
    version = log_and_register(result, settings, data_version)
    decision = promote(settings, version, result.split.validation)
    _log.info(
        "model_trained",
        version=version,
        promoted=decision.promoted,
        reason=decision.reason,
        delayed_15_pr_auc=round(result.metrics["delayed_15_pr_auc"], 4),
        delayed_180_pr_auc=round(result.metrics["delayed_180_pr_auc"], 4),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
