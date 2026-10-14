from pathlib import Path

from skypulse.reference.ourairports import (
    parse_airports,
    parse_countries,
    parse_runway_headings,
)

FIXTURES = Path(__file__).parents[3] / "data" / "fixtures" / "ourairports"

RUNWAYS_HEADER = (
    '"id","airport_ref","airport_ident","length_ft","width_ft","surface","lighted","closed",'
    '"le_ident","le_latitude_deg","le_longitude_deg","le_elevation_ft","le_heading_degT",'
    '"le_displaced_threshold_ft","he_ident","he_latitude_deg","he_longitude_deg",'
    '"he_elevation_ft","he_heading_degT","he_displaced_threshold_ft"\n'
)


def runway(ident: str, le: str, le_heading: str, he_heading: str, closed: str = "0") -> str:
    return f'1,1,"{ident}",1,1,"ASP",1,{closed},"{le}",,,,{le_heading},,"x",,,,{he_heading},\n'


def test_fixture_airports_include_the_monitored_ones() -> None:
    airports = {row.icao: row for row in parse_airports((FIXTURES / "airports.csv").read_text())}

    assert airports["EIDW"].country == "IE"
    assert airports["KJFK"].country == "US"
    assert airports["LEPA"].iata == "PMI"


def test_runway_heading_comes_from_data_then_from_the_runway_number() -> None:
    text = RUNWAYS_HEADER + runway("LEPA", "06L", "58.6", "") + runway("LXXX", "24R", "", "")

    headings = parse_runway_headings(text)

    assert headings == {"LEPA": [59], "LXXX": [240]}


def test_closed_runways_are_ignored() -> None:
    text = RUNWAYS_HEADER + runway("EGSS", "04", "44", "224", closed="1")

    assert parse_runway_headings(text) == {}


def test_fixture_runways_cover_palma() -> None:
    headings = parse_runway_headings((FIXTURES / "runways.csv").read_text())

    assert headings["LEPA"]


def test_countries_carry_the_eu261_scope_flag() -> None:
    countries = {
        code: in_scope
        for code, _, _, in_scope in parse_countries((FIXTURES / "countries.csv").read_text())
    }

    assert countries["IE"] is True
    assert countries["CH"] is True
    assert countries["US"] is False
