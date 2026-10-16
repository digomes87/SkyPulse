import os
import time
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from skypulse.common.config import Settings
from skypulse.streaming.session import Connectors, build_session

# MLflow prints an advert for its tracing skill on import; keep test output clean.
os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")


@pytest.fixture(scope="session")
def utc_process() -> None:
    """collect() renders timestamps in the Python process time zone; pin it."""
    os.environ["TZ"] = "UTC"
    time.tzset()


@pytest.fixture(scope="session")
def spark(utc_process: None) -> Iterator[SparkSession]:
    session = build_session("skypulse-tests", Settings(), Connectors(), master="local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
