"""Recomputes every EU261 assessment from the announced disruptions.

The same pure ``skypulse.eu261`` function the calculator Lambda uses, mapped over
``silver/flight_disruptions`` with Spark. The output replaces ``silver/eu261_assessments``
on every run, so a rule change or a fixed bug is applied to history by rerunning the job.
"""

import json
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, Row, SparkSession, Window, functions

from skypulse.common.config import LakeLayer, Settings
from skypulse.common.logging import get_logger
from skypulse.lambdas.claims import Claim
from skypulse.lambdas.disruption import Disruption
from skypulse.streaming.detector import disruptions_path

ASSESSMENTS_TABLE = "eu261_assessments"
# Final outcomes win over early warnings for the same flight.
KIND_PRIORITY = {"cancellation": 2, "arrival_delay": 2, "departure_delay": 1}
ASSESSMENT_SCHEMA = (
    "claim_id string, source_event_id string, flight_number string, flight_date string, "
    "scheduled_departure_utc string, kind string, origin string, destination string, "
    "airline_icao string, verdict string, distance_band string, distance_km double, "
    "amount_eur double, reduced boolean, extraordinary_circumstances_likely boolean, "
    "severe_weather array<string>, arrival_delay_minutes int, detected_at string, "
    "assessed_at string"
)
FIELDS = [field.split()[0] for field in ASSESSMENT_SCHEMA.split(", ")]

_log = get_logger(__name__)


def assessment_row(payload: str, assessed_at: datetime) -> tuple[Any, ...] | None:
    disruption = Disruption.from_document(json.loads(payload))
    if not disruption.assessable():
        return None
    item = dict(Claim.from_assessment(disruption, disruption.assess(), assessed_at, None).item)
    item["scheduled_departure_utc"] = disruption.data["scheduled_departure_utc"]
    item["distance_km"] = float(item["distance_km"])
    item["amount_eur"] = float(item["amount_eur"])
    return tuple(item.get(field) for field in FIELDS)


def assess_disruptions(disruptions: DataFrame, assessed_at: datetime) -> DataFrame:
    spark = disruptions.sparkSession

    def assess(row: Row) -> list[tuple[Any, ...]]:
        assessed = assessment_row(row["payload"], assessed_at)
        return [] if assessed is None else [assessed]

    rows = disruptions.select("payload").rdd.flatMap(assess)
    return latest_per_claim(spark.createDataFrame(rows, ASSESSMENT_SCHEMA))


def latest_per_claim(assessments: DataFrame) -> DataFrame:
    priority = functions.create_map(
        *[functions.lit(value) for pair in KIND_PRIORITY.items() for value in pair]
    )
    window = Window.partitionBy("claim_id").orderBy(
        priority[functions.col("kind")].desc(), functions.col("detected_at").desc()
    )
    ranked = assessments.withColumn("_rank", functions.row_number().over(window))
    return ranked.where("_rank = 1").drop("_rank")


def assessments_path(settings: Settings) -> str:
    return f"{settings.lake.uri_for(LakeLayer.SILVER)}/{ASSESSMENTS_TABLE}"


def recompute_assessments(spark: SparkSession, settings: Settings) -> int:
    disruptions = spark.read.format("delta").load(disruptions_path(settings))
    assessments = assess_disruptions(disruptions, datetime.now(UTC)).cache()
    assessments.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        assessments_path(settings)
    )
    count = assessments.count()
    assessments.unpersist()
    _log.info("assessments_recomputed", count=count)
    return count
