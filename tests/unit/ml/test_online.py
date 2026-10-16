"""Online features must equal the offline ones for the same flight and inputs."""

from datetime import UTC, datetime

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from skypulse.ml.feature_spec import FEATURES
from skypulse.ml.features import build_features
from skypulse.ml.online import FlightQuery, Statistics, WeatherReport
from skypulse.streaming.disruptions import reference_airports
from tests.unit.streaming.schedule_rows import METAR_SCHEMA, SCHEDULE_SCHEMA


def spark_time(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 10, hour, minute)  # noqa: DTZ001 - Spark session runs in UTC


@pytest.mark.spark
def test_online_and_offline_features_agree(spark: SparkSession) -> None:
    flight = ("FR1", "RYR", "EIDW", "LEMD", spark_time(9), spark_time(11),
              None, None, None, None, "Expected", spark_time(9), "c")  # fmt: skip
    metar = [
        ("EIDW", spark_time(6, 50), "+TSRA", 1500, 240, 25, 40),
        ("LEMD", spark_time(6, 30), "-RA", 6000, 200, 8, None),
    ]
    offline = build_features(
        spark.createDataFrame([flight], SCHEDULE_SCHEMA),
        spark.createDataFrame(metar, METAR_SCHEMA),
        reference_airports(spark),
    ).toPandas()[list(FEATURES)]

    online = FlightQuery(
        "EIDW",
        "LEMD",
        "RYR",
        datetime(2026, 10, 10, 9, tzinfo=UTC),
        WeatherReport(1500, 25, 40, "+TSRA"),
        WeatherReport(6000, 8, None, "-RA"),
    ).features(Statistics.empty())

    pd.testing.assert_frame_equal(
        online.reset_index(drop=True),
        offline.reset_index(drop=True),
        check_dtype=False,
        atol=1e-6,
    )
