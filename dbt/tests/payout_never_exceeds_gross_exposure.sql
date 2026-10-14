-- Expected payout plus likely-exempt amount must equal the gross exposure per airline.
select airline_icao
from {{ ref('agg_eu261_exposure_by_airline') }}
where abs(expected_payout_eur + likely_exempt_eur - gross_exposure_eur) > 0.001
