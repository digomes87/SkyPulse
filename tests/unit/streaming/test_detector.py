from collections.abc import Iterable
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession, functions

from skypulse.common.config import Settings
from skypulse.streaming.announcements import Announcement
from skypulse.streaming.detector import detect_batch, disruptions_path
from tests.unit.streaming.schedule_rows import at, schedule, schedules

pytestmark = pytest.mark.spark


class CollectingAnnouncer:
    def __init__(self) -> None:
        self.announced: list[Announcement] = []

    def announce(self, announcements: Iterable[Announcement]) -> int:
        batch = list(announcements)
        self.announced.extend(batch)
        return len(batch)


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("SKYPULSE_LAKE__ROOT_URI", tmp_path.as_uri())
    return Settings()


def change_feed(
    spark: SparkSession, *rows: tuple[object, ...], change: str = "update_postimage"
) -> DataFrame:
    return schedules(spark, *rows).withColumn("_change_type", functions.lit(change))


def test_disruptions_are_announced_once(spark: SparkSession, settings: Settings) -> None:
    announcer = CollectingAnnouncer()
    cancelled = schedule("FR1234", "Canceled", scheduled=(at(9, 15), at(11, 45)))
    late = schedule("EI105", "Arrived", route=("EIDW", "KJFK"), scheduled=(at(7, 30), at(14, 30)),
                    runway=(at(11, 40), at(18, 40)), airline="EIN")  # fmt: skip

    first = detect_batch(change_feed(spark, cancelled, late), settings, announcer)
    second = detect_batch(change_feed(spark, cancelled, late), settings, announcer)

    assert (first, second) == (2, 0)
    kinds = {
        item.event.data["flight_number"]: item.event.data["kind"] for item in announcer.announced
    }
    assert kinds == {"FR1234": "cancellation", "EI105": "arrival_delay"}
    recorded = spark.read.format("delta").load(disruptions_path(settings))
    assert recorded.count() == 2


def test_pre_images_and_quiet_flights_are_ignored(spark: SparkSession, settings: Settings) -> None:
    announcer = CollectingAnnouncer()
    batch = change_feed(spark, schedule("FR1", "Canceled"), change="update_preimage").unionByName(
        change_feed(spark, schedule("FR2", "Expected"))
    )

    assert detect_batch(batch, settings, announcer) == 0
    assert announcer.announced == []


def test_announced_payload_has_utc_times(spark: SparkSession, settings: Settings) -> None:
    announcer = CollectingAnnouncer()

    detect_batch(
        change_feed(spark, schedule("FR1234", "Canceled", scheduled=(at(9, 15), at(11, 45)))),
        settings,
        announcer,
    )

    data = announcer.announced[0].event.data
    assert data["scheduled_departure_utc"] == "2026-10-14T09:15:00Z"
    assert data["origin"]["country"] == "IE"
    assert announcer.announced[0].correlation_id == "cid-1"
