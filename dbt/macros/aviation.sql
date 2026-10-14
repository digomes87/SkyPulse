{# Great-circle distance in km (haversine), the method EU261 Art. 7(4) prescribes. #}
{% macro haversine_km(lat1, lon1, lat2, lon2) -%}
    2 * 6371.0 * asin(sqrt(
        power(sin(radians({{ lat2 }} - {{ lat1 }}) / 2), 2)
        + cos(radians({{ lat1 }})) * cos(radians({{ lat2 }}))
        * power(sin(radians({{ lon2 }} - {{ lon1 }}) / 2), 2)
    ))
{%- endmacro %}

{# EU261 compensation band, mirroring skypulse.eu261.rules.Route.band. #}
{% macro eu261_band(distance_km, is_intra_eu261) -%}
    case
        when {{ distance_km }} <= 1500 then 'short'
        when {{ is_intra_eu261 }} or {{ distance_km }} <= 3500 then 'medium'
        else 'long'
    end
{%- endmacro %}

{# Minutes between two timestamps (later - earlier). #}
{% macro minutes_between(earlier, later) -%}
    {{ dbt.datediff(earlier, later, 'minute') }}
{%- endmacro %}
