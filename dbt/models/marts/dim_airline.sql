select
    airlines.airline_icao,
    airlines.airline_iata,
    airlines.airline_name,
    airlines.country_code,
    coalesce(countries.in_eu261_scope, false) as in_eu261_scope
from {{ ref('stg_reference_airlines') }} as airlines
left join {{ ref('stg_reference_countries') }} as countries
    on airlines.country_code = countries.country_code
