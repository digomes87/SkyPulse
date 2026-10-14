from skypulse.common.aviation import AirportIcao
from skypulse.ingestion.airports import MonitoredAirports


def test_monitored_airports_load_with_timezones() -> None:
    airports = MonitoredAirports.load()

    assert AirportIcao("EIDW") in airports.codes()
    assert all(airport.timezone.key for airport in airports)
    assert len(airports) == len(set(airports.codes()))
