"""Great Expectations suites, defined in code.

Split of responsibilities: dbt tests guard the structure of the gold models (keys,
relationships, accepted values); these suites guard plausibility. On silver they check
formats, ranges and natural-key uniqueness right after the streaming jobs, before anything
is built on top. On gold they check business-level distributions that a broken feed would
shift (cancellation share, typical delays, on-time rates).
"""

from collections.abc import Callable, Mapping

import great_expectations as gx
from great_expectations.expectations import Expectation

ICAO_AIRPORT = r"^[A-Z0-9]{4}$"
ICAO24 = r"^[0-9a-f]{6}$"
FLIGHT_STATUSES = [
    "Unknown", "Expected", "EnRoute", "CheckIn", "Boarding", "GateClosed", "Departed",
    "Delayed", "Approaching", "Arrived", "Canceled", "Diverted", "CanceledUncertain",
]  # fmt: skip
EU261_AMOUNTS = [0, 125, 200, 250, 300, 400, 600]

ex = gx.expectations
SuiteFactory = Callable[[], list[Expectation]]


def _rows_present() -> Expectation:
    return ex.ExpectTableRowCountToBeBetween(min_value=1)


def _aircraft_states() -> list[Expectation]:
    return [
        _rows_present(),
        ex.ExpectColumnValuesToMatchRegex(column="icao24", regex=ICAO24),
        ex.ExpectCompoundColumnsToBeUnique(column_list=["icao24", "last_contact"]),
        ex.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90, max_value=90),
        ex.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180, max_value=180),
        ex.ExpectColumnValuesToBeBetween(
            column="velocity", min_value=0, max_value=400, mostly=0.99
        ),
    ]


def _flight_schedules() -> list[Expectation]:
    return [
        _rows_present(),
        ex.ExpectColumnValuesToNotBeNull(column="flight_number"),
        ex.ExpectCompoundColumnsToBeUnique(
            column_list=["flight_number", "scheduled_departure_utc"]
        ),
        ex.ExpectColumnValuesToMatchRegex(column="origin_icao", regex=ICAO_AIRPORT),
        ex.ExpectColumnValuesToMatchRegex(column="destination_icao", regex=ICAO_AIRPORT),
        ex.ExpectColumnValuesToBeInSet(column="status", value_set=FLIGHT_STATUSES),
        ex.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="scheduled_arrival_utc",
            column_B="scheduled_departure_utc",
            ignore_row_if="either_value_is_missing",
        ),
    ]


def _metar_observations() -> list[Expectation]:
    return [
        _rows_present(),
        ex.ExpectColumnValuesToMatchRegex(column="station_icao", regex=ICAO_AIRPORT),
        ex.ExpectCompoundColumnsToBeUnique(
            column_list=["station_icao", "observed_at", "report_type"]
        ),
        ex.ExpectColumnValuesToBeBetween(column="visibility_m", min_value=0, max_value=9999),
        ex.ExpectColumnValuesToBeBetween(column="wind_speed_kt", min_value=0, max_value=150),
        ex.ExpectColumnValuesToBeBetween(column="temperature_c", min_value=-60, max_value=60),
    ]


def _flight_disruptions() -> list[Expectation]:
    return [
        ex.ExpectColumnValuesToBeInSet(
            column="kind", value_set=["departure_delay", "arrival_delay", "cancellation"]
        ),
        ex.ExpectCompoundColumnsToBeUnique(
            column_list=["flight_number", "scheduled_departure_utc", "kind"]
        ),
    ]


def _gold_flights() -> list[Expectation]:
    return [
        _rows_present(),
        ex.ExpectColumnValuesToBeUnique(column="flight_key"),
        # A day where more than a fifth of flights are cancelled is far more likely a feed
        # problem than reality; median delays above two hours likewise.
        ex.ExpectColumnMeanToBeBetween(column="is_cancelled", min_value=0, max_value=0.2),
        ex.ExpectColumnMedianToBeBetween(
            column="departure_delay_minutes", min_value=-30, max_value=120
        ),
        ex.ExpectColumnValuesToBeBetween(
            column="arrival_delay_minutes", min_value=-120, max_value=1440
        ),
    ]


def _gold_claims() -> list[Expectation]:
    return [
        ex.ExpectColumnValuesToBeUnique(column="claim_id"),
        ex.ExpectColumnValuesToBeInSet(column="amount_eur", value_set=EU261_AMOUNTS),
        ex.ExpectColumnValuesToNotBeNull(column="flight_key"),
    ]


def _gold_airport_delays() -> list[Expectation]:
    return [
        ex.ExpectColumnValuesToBeBetween(column="on_time_rate", min_value=0, max_value=1),
        ex.ExpectColumnValuesToBeBetween(column="departures", min_value=1),
    ]


SILVER_SUITES: Mapping[str, SuiteFactory] = {
    "aircraft_states": _aircraft_states,
    "flight_schedules": _flight_schedules,
    "metar_observations": _metar_observations,
    "flight_disruptions": _flight_disruptions,
}

GOLD_SUITES: Mapping[str, SuiteFactory] = {
    "fct_flight": _gold_flights,
    "fct_eu261_claim": _gold_claims,
    "agg_airport_daily_delays": _gold_airport_delays,
}
