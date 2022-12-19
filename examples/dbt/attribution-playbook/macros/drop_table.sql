{%- macro drop_table(table_name) -%}
    {%- set drop_query -%}
        DROP TABLE IF EXISTS {{ target.schema }}.{{ table_name }}
    {%- endset -%}
    {% do run_query(drop_query) %}
{%- endmacro -%}
