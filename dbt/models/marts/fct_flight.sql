{{ config(liquid_clustered_by=['flight_date', 'origin_icao'] if target.type == 'databricks' else none) }}

with flights as (
    select
        *,
        coalesce(runway_departure_utc, revised_departure_utc) as actual_departure_utc,
        case
            when runway_arrival_utc is not null or status = 'Arrived'
                then coalesce(runway_arrival_utc, revised_arrival_utc)
        end as actual_arrival_utc,
        status in ('Canceled', 'CanceledUncertain') as is_cancelled
    from {{ ref('stg_flight_schedules') }}
),

measured as (
    select
        *,
        {{ minutes_between('scheduled_departure_utc', 'actual_departure_utc') }} as departure_delay_minutes,
        {{ minutes_between('scheduled_arrival_utc', 'actual_arrival_utc') }} as arrival_delay_minutes
    from flights
)

select
    flight_key,
    flight_number,
    airline_icao,
    {{ dbt_utils.generate_surrogate_key(['origin_icao', 'destination_icao']) }} as route_key,
    origin_icao,
    destination_icao,
    cast(scheduled_departure_utc as date) as flight_date,
    scheduled_departure_utc,
    scheduled_arrival_utc,
    actual_departure_utc,
    actual_arrival_utc,
    departure_delay_minutes,
    arrival_delay_minutes,
    status,
    is_cancelled,
    actual_arrival_utc is not null as has_arrived,
    actual_arrival_utc is not null
        and arrival_delay_minutes < {{ var('on_time_threshold_minutes') }} as is_on_time,
    case
        when is_cancelled then 'cancelled'
        when actual_arrival_utc is null then 'not_arrived'
        when arrival_delay_minutes < 15 then 'on_time'
        when arrival_delay_minutes < 60 then '15_to_59'
        when arrival_delay_minutes < {{ var('eu261_delay_threshold_minutes') }} then '60_to_179'
        else '180_plus'
    end as delay_band,
    observed_at as last_observed_at
from measured
