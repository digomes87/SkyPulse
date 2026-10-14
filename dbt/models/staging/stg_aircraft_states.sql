select
    icao24,
    callsign,
    origin_country,
    to_timestamp(last_contact) as last_contact_at,
    latitude,
    longitude,
    coalesce(geo_altitude, baro_altitude) as altitude_m,
    velocity as ground_speed_ms,
    true_track as track_degrees,
    on_ground
from {{ source('silver', 'aircraft_states') }}
where latitude is not null and longitude is not null
