select
    station_icao,
    observed_at,
    report_type,
    wind_direction_degrees,
    wind_variable,
    wind_speed_kt,
    wind_gust_kt,
    visibility_m,
    present_weather,
    temperature_c,
    flight_category
from {{ source('silver', 'metar_observations') }}
