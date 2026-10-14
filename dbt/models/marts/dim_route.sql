with routes as (
    select distinct origin_icao, destination_icao
    from {{ ref('stg_flight_schedules') }}
),

measured as (
    select
        {{ dbt_utils.generate_surrogate_key(['routes.origin_icao', 'routes.destination_icao']) }} as route_key,
        routes.origin_icao,
        routes.destination_icao,
        routes.origin_icao || '-' || routes.destination_icao as route_name,
        {{ haversine_km('origin.latitude', 'origin.longitude', 'destination.latitude', 'destination.longitude') }} as distance_km,
        origin.in_eu261_scope and destination.in_eu261_scope as is_intra_eu261
    from routes
    left join {{ ref('dim_airport') }} as origin on routes.origin_icao = origin.airport_icao
    left join {{ ref('dim_airport') }} as destination on routes.destination_icao = destination.airport_icao
)

select
    *,
    {{ eu261_band('distance_km', 'is_intra_eu261') }} as eu261_distance_band
from measured
