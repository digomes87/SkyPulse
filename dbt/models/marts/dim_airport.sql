{{ config(liquid_clustered_by=['country_code'] if target.type == 'databricks' else none) }}

select
    airports.airport_icao,
    airports.airport_iata,
    airports.airport_name,
    airports.airport_type,
    airports.municipality,
    airports.country_code,
    countries.country_name,
    countries.continent,
    coalesce(countries.in_eu261_scope, false) as in_eu261_scope,
    airports.latitude,
    airports.longitude,
    airports.runway_count
from {{ ref('stg_reference_airports') }} as airports
left join {{ ref('stg_reference_countries') }} as countries
    on airports.country_code = countries.country_code
