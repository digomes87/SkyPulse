select
    icao as airport_icao,
    iata as airport_iata,
    name as airport_name,
    airport_type,
    country as country_code,
    municipality,
    latitude,
    longitude,
    {{ skypulse_array_size('runway_headings') }} as runway_count
from {{ source('silver', 'reference_airports') }}
