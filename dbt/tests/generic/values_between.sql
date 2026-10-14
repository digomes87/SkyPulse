{# Fails for rows where the column is outside [min_value, max_value]. Nulls pass. #}
{% test values_between(model, column_name, min_value, max_value) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}
