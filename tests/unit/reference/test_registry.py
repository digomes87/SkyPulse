from skypulse.ingestion.airports import MonitoredAirports
from skypulse.reference.registry import airlines, airports


def test_every_monitored_airport_has_reference_data() -> None:
    for code in MonitoredAirports.load().codes():
        assert airports().find(str(code)) is not None


def test_airports_have_coordinates_and_country() -> None:
    dublin = airports().find("EIDW")

    assert dublin is not None
    assert dublin["country"] == "IE"
    assert -90 <= dublin["latitude"] <= 90


def test_airline_lookup() -> None:
    ryanair = airlines().find("RYR")

    assert ryanair is not None
    assert ryanair["country"] == "IE"
    assert airlines().find(None) is None
    assert airlines().find("ZZZ") is None
    assert len(airlines()) >= 6
