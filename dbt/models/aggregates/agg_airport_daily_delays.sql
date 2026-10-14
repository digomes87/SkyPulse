{{ config(liquid_clustered_by=['flight_date'] if target.type == 'databricks' else none) }}

select
    origin_icao as airport_icao,
    flight_date,
    count(*) as departures,
    sum(case when is_cancelled then 1 else 0 end) as cancelled,
    sum(case when departure_delay_minutes >= 15 then 1 else 0 end) as delayed_15_plus,
    sum(case when arrival_delay_minutes >= {{ var('eu261_delay_threshold_minutes') }} then 1 else 0 end) as arrived_180_plus_late,
    avg(departure_delay_minutes) as avg_departure_delay_minutes,
    {{ skypulse_percentile('departure_delay_minutes', 0.9) }} as p90_departure_delay_minutes,
    avg(case when has_arrived then case when is_on_time then 1.0 else 0.0 end end) as on_time_rate
from {{ ref('fct_flight') }}
group by origin_icao, flight_date
