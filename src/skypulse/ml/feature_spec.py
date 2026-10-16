"""The feature contract shared by the offline feature job, training and online serving.

Point in time: a prediction is made ``PREDICTION_LEAD`` before the scheduled departure.
Every feature must be knowable at that moment: weather is the latest METAR reported
before it, and historical delay statistics only use flights that had already landed.
"""

from datetime import timedelta

FEATURE_SET_VERSION = "v1"
PREDICTION_LEAD = timedelta(hours=2)
WEATHER_LOOKBACK = timedelta(hours=3)
HISTORY_WINDOW_DAYS = 30

CATEGORICAL_FEATURES = ("origin_icao", "destination_icao", "airline_icao")
NUMERIC_FEATURES = (
    "departure_hour_utc",
    "day_of_week",
    "distance_km",
    "origin_visibility_m",
    "origin_wind_kt",
    "origin_gust_kt",
    "origin_thunderstorm",
    "origin_precipitation",
    "destination_visibility_m",
    "destination_wind_kt",
    "destination_thunderstorm",
    "route_mean_arrival_delay",
    "route_delay_15_rate",
    "route_flights_30d",
    "airline_delay_15_rate",
)
FEATURES = (*CATEGORICAL_FEATURES, *NUMERIC_FEATURES)

# Present-weather groups, matched the same way offline (Spark rlike) and online (re.search).
THUNDERSTORM_PATTERN = "TS"
PRECIPITATION_PATTERN = "RA|SN|DZ|PL|GR"

LABELS = {"delayed_15": 15, "delayed_180": 180}
KEY_COLUMNS = ("flight_number", "scheduled_departure_utc")
