from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from skypulse.common.config import Settings
from skypulse.streaming.session import Connectors, build_session


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = build_session("skypulse-tests", Settings(), Connectors(), master="local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
