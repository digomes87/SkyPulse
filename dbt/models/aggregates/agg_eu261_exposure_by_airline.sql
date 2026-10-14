with flights as (
    select airline_icao, count(*) as flights
    from {{ ref('fct_flight') }}
    group by airline_icao
),

claims as (
    select
        airline_icao,
        count(*) as disruptions_assessed,
        sum(case when is_eligible then 1 else 0 end) as eligible_claims,
        sum(case when is_eligible then amount_eur else 0 end) as gross_exposure_eur,
        sum(likely_exempt_eur) as likely_exempt_eur,
        sum(expected_payout_eur) as expected_payout_eur
    from {{ ref('fct_eu261_claim') }}
    group by airline_icao
)

select
    flights.airline_icao,
    airlines.airline_name,
    flights.flights,
    coalesce(claims.disruptions_assessed, 0) as disruptions_assessed,
    coalesce(claims.eligible_claims, 0) as eligible_claims,
    coalesce(claims.gross_exposure_eur, 0) as gross_exposure_eur,
    coalesce(claims.likely_exempt_eur, 0) as likely_exempt_eur,
    coalesce(claims.expected_payout_eur, 0) as expected_payout_eur,
    coalesce(claims.expected_payout_eur, 0) * 100.0 / flights.flights as expected_payout_per_100_flights_eur
from flights
left join claims on flights.airline_icao = claims.airline_icao
left join {{ ref('dim_airline') }} as airlines on flights.airline_icao = airlines.airline_icao
