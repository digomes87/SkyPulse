select
    icao as airline_icao,
    iata as airline_iata,
    name as airline_name,
    country as country_code
from {{ source('silver', 'reference_airlines') }}
