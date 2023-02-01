{%- macro drop_table(table_name, conn_type) -%}
    {% if conn_type != 'bigquery' %}
      {%- set drop_query -%}
          DROP TABLE IF EXISTS {{ target.schema }}.{{ table_name }} CASCADE
      {%- endset -%}
    {% else %}
      {%- set drop_query -%}
          DROP TABLE IF EXISTS {{ target.schema }}.{{ table_name }}
      {%- endset -%}
    {% endif %}
    {% do run_query(drop_query) %}
{%- endmacro -%}
