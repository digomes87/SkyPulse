{{ config(liquid_clustered_by=['airline_icao'] if target.type == 'databricks' else none) }}

select
    claims.claim_id,
    flights.flight_key,
    claims.flight_number,
    claims.airline_icao,
    {{ dbt_utils.generate_surrogate_key(['claims.origin_icao', 'claims.destination_icao']) }} as route_key,
    claims.origin_icao,
    claims.destination_icao,
    cast(claims.scheduled_departure_utc as date) as flight_date,
    claims.kind as disruption_kind,
    claims.verdict,
    claims.verdict = 'eligible' as is_eligible,
    claims.distance_band,
    claims.distance_km,
    claims.arrival_delay_minutes,
    claims.amount_eur,
    claims.reduced as is_reduced,
    claims.extraordinary_circumstances_likely,
    claims.severe_weather,
    case
        when claims.verdict = 'eligible' and claims.extraordinary_circumstances_likely
            then claims.amount_eur
        else 0
    end as likely_exempt_eur,
    case
        when claims.verdict = 'eligible' and not claims.extraordinary_circumstances_likely
            then claims.amount_eur
        else 0
    end as expected_payout_eur,
    claims.detected_at
from {{ ref('stg_eu261_assessments') }} as claims
left join {{ ref('stg_flight_schedules') }} as flights
    on claims.flight_number = flights.flight_number
    and claims.scheduled_departure_utc = flights.scheduled_departure_utc
