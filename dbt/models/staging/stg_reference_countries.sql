select
    code as country_code,
    name as country_name,
    continent,
    in_eu261_scope
from {{ source('silver', 'reference_countries') }}
