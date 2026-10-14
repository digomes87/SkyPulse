from datetime import UTC, datetime, timedelta

import pytest

from skypulse.common.aviation import AirportIcao, TimeWindow

START = datetime(2026, 10, 14, tzinfo=UTC)


@pytest.mark.parametrize("code", ["eidw", "DUB", "EIDWX", ""])
def test_airport_icao_rejects_malformed_codes(code: str) -> None:
    with pytest.raises(ValueError, match="ICAO"):
        AirportIcao(code)


def test_time_window_must_move_forward() -> None:
    with pytest.raises(ValueError, match="after"):
        TimeWindow(START, START)


def test_time_window_splits_into_bounded_chunks() -> None:
    window = TimeWindow(START, START + timedelta(hours=30))

    chunks = window.split(timedelta(hours=12))

    assert [chunk.duration() for chunk in chunks] == [
        timedelta(hours=12),
        timedelta(hours=12),
        timedelta(hours=6),
    ]
    assert chunks[-1].end == window.end
