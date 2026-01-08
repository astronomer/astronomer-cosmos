{% macro format_currency(amount) %}
    '$' || cast({{ amount }} as varchar)
{% endmacro %}
