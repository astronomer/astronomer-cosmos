{%- macro drop_table_by_name(table_name) -%}
    {%- set drop_query -%}
        DROP TABLE IF EXISTS {{ target.schema }}.{{ table_name }} CASCADE
    {%- endset -%}
    {% do run_query(drop_query) %}
{%- endmacro -%}
