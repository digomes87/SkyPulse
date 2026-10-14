select
    {{ dbt_utils.generate_surrogate_key(['flight_number', 'scheduled_departure_utc']) }} as flight_key,
    flight_number,
    callsign,
    airline_icao,
    airline_iata,
    airline_name,
    origin_icao,
    destination_icao,
    scheduled_departure_utc,
    scheduled_arrival_utc,
    revised_departure_utc,
    revised_arrival_utc,
    runway_departure_utc,
    runway_arrival_utc,
    status,
    is_cargo,
    observed_at
from {{ source('silver', 'flight_schedules') }}
