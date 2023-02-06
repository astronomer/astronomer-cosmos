{% macro grant_select_on_schemas(schemas, role) %}
  {% for schema in schemas %}
    grant usage on schema {{ schema }} to role {{ role }};
    grant select on all tables in schema {{ schema }} to role {{ role }};
    grant select on all views in schema {{ schema }} to role {{ role }};
    grant select on future tables in schema {{ schema }} to role {{ role }};
    grant select on future views in schema {{ schema }} to role {{ role }};
  {% endfor %}
{% endmacro %}
