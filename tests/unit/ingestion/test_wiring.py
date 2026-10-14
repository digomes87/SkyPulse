from pathlib import Path

import pytest
from pydantic import SecretStr

from skypulse.common.config import IngestionSettings, OpenSkySettings, RunMode, Settings
from skypulse.common.streams import InMemoryStream
from skypulse.ingestion.__main__ import parse_arguments
from skypulse.ingestion.pollers import AircraftStatePoller, MetarPoller, SchedulePoller
from skypulse.ingestion.wiring import Feed, MissingCredentialsError, Wiring


def settings(replay: bool, **overrides: object) -> Settings:
    ingestion = IngestionSettings(replay=replay, fixtures_dir=Path("/nonexistent"))
    return Settings.model_validate({"ingestion": ingestion, **overrides})


@pytest.mark.parametrize(
    ("feed", "poller_type"),
    [
        (Feed.STATES, AircraftStatePoller),
        (Feed.SCHEDULES, SchedulePoller),
        (Feed.METAR, MetarPoller),
    ],
)
def test_replay_mode_builds_every_poller(feed: Feed, poller_type: type) -> None:
    wiring = Wiring(settings(replay=True), InMemoryStream())

    assert isinstance(wiring.poller(feed), poller_type)


def test_live_schedules_need_an_api_key() -> None:
    wiring = Wiring(settings(replay=False), InMemoryStream())

    with pytest.raises(MissingCredentialsError, match="AERODATABOX"):
        wiring.poller(Feed.SCHEDULES)


def test_live_mode_with_credentials_builds_authenticated_pollers() -> None:
    opensky = OpenSkySettings(client_id="id", client_secret=SecretStr("secret"))
    wiring = Wiring(settings(replay=False, opensky=opensky), InMemoryStream())

    assert isinstance(wiring.poller(Feed.STATES), AircraftStatePoller)


def test_aws_mode_is_not_wired_yet() -> None:
    with pytest.raises(NotImplementedError, match="M8"):
        Wiring.from_settings(settings(replay=True, run_mode=RunMode.AWS))


def test_each_feed_has_its_own_interval() -> None:
    configured = settings(replay=True)

    assert Feed.STATES.interval(configured) == 10.0
    assert Feed.METAR.interval(configured) == 600.0


def test_cli_parses_feed_and_cycles() -> None:
    arguments = parse_arguments(["metar", "--cycles", "2"])

    assert arguments.feed is Feed.METAR
    assert arguments.cycles == 2
