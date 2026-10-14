with flights as (
    select
        route_key,
        count(*) as flights,
        sum(case when has_arrived then 1 else 0 end) as arrived,
        sum(case when is_on_time then 1 else 0 end) as on_time,
        sum(case when is_cancelled then 1 else 0 end) as cancelled,
        avg(arrival_delay_minutes) as avg_arrival_delay_minutes,
        max(arrival_delay_minutes) as max_arrival_delay_minutes
    from {{ ref('fct_flight') }}
    group by route_key
)

select
    routes.route_key,
    routes.route_name,
    routes.origin_icao,
    routes.destination_icao,
    routes.distance_km,
    routes.eu261_distance_band,
    flights.flights,
    flights.arrived,
    flights.on_time,
    flights.cancelled,
    case when flights.arrived > 0 then flights.on_time * 1.0 / flights.arrived end as on_time_performance,
    flights.avg_arrival_delay_minutes,
    flights.max_arrival_delay_minutes
from flights
inner join {{ ref('dim_route') }} as routes on flights.route_key = routes.route_key
