{# Small functions whose names differ between DuckDB (local) and Spark SQL (Databricks). #}

{% macro skypulse_array_join(column, separator) -%}
    {{ return(adapter.dispatch('skypulse_array_join')(column, separator)) }}
{%- endmacro %}

{% macro default__skypulse_array_join(column, separator) -%}
    array_join({{ column }}, {{ separator }})
{%- endmacro %}

{% macro duckdb__skypulse_array_join(column, separator) -%}
    array_to_string({{ column }}, {{ separator }})
{%- endmacro %}

{% macro skypulse_array_size(column) -%}
    {{ return(adapter.dispatch('skypulse_array_size')(column)) }}
{%- endmacro %}

{% macro default__skypulse_array_size(column) -%}
    size({{ column }})
{%- endmacro %}

{% macro duckdb__skypulse_array_size(column) -%}
    len({{ column }})
{%- endmacro %}

{% macro skypulse_percentile(column, fraction) -%}
    {{ return(adapter.dispatch('skypulse_percentile')(column, fraction)) }}
{%- endmacro %}

{% macro default__skypulse_percentile(column, fraction) -%}
    percentile_approx({{ column }}, {{ fraction }})
{%- endmacro %}

{% macro duckdb__skypulse_percentile(column, fraction) -%}
    quantile_cont({{ column }}, {{ fraction }})
{%- endmacro %}
