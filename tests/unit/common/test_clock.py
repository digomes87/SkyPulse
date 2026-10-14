from datetime import UTC, datetime, timedelta, timezone

import pytest

from skypulse.common.clock import FrozenClock, SystemClock, from_unix, utc_iso


def test_utc_iso_normalises_offsets_to_z() -> None:
    dublin_summer = timezone(timedelta(hours=1))

    assert utc_iso(datetime(2026, 7, 1, 10, 0, 30, 999, tzinfo=dublin_summer)) == (
        "2026-07-01T09:00:30Z"
    )


def test_utc_iso_rejects_naive_datetimes() -> None:
    with pytest.raises(ValueError, match="timezone"):
        utc_iso(datetime(2026, 7, 1))  # noqa: DTZ001


def test_from_unix_is_utc() -> None:
    assert from_unix(0) == datetime(1970, 1, 1, tzinfo=UTC)


def test_frozen_clock_returns_the_same_instant() -> None:
    instant = datetime(2026, 10, 14, tzinfo=UTC)

    assert FrozenClock(instant).now() == instant


def test_system_clock_is_timezone_aware() -> None:
    assert SystemClock().now().tzinfo is not None
