select
    claim_id,
    source_event_id,
    flight_number,
    cast(scheduled_departure_utc as timestamp) as scheduled_departure_utc,
    kind,
    origin as origin_icao,
    destination as destination_icao,
    airline_icao,
    verdict,
    distance_band,
    distance_km,
    amount_eur,
    reduced,
    extraordinary_circumstances_likely,
    {{ skypulse_array_join('severe_weather', "', '") }} as severe_weather,
    arrival_delay_minutes,
    cast(detected_at as timestamp) as detected_at
from {{ source('silver', 'eu261_assessments') }}
