-- Latest known position per aircraft: the airspace picture at the end of the data.
with ranked as (
    select
        *,
        row_number() over (partition by icao24 order by last_contact_at desc) as recency
    from {{ ref('stg_aircraft_states') }}
)

select
    icao24,
    callsign,
    origin_country,
    last_contact_at,
    latitude,
    longitude,
    altitude_m,
    ground_speed_ms,
    track_degrees,
    on_ground
from ranked
where recency = 1
