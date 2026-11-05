{%- macro drop_table_by_name(table_name) -%}
    {{ return(adapter.dispatch('drop_table_by_name')(table_name)) }}
{%- endmacro -%}

{#- BigQuery does not support CASCADE; Postgres needs it to drop the views that depend on a seed. -#}
{%- macro default__drop_table_by_name(table_name) -%}
    {%- set drop_query -%}
        DROP TABLE IF EXISTS {{ target.schema }}.{{ table_name }}
    {%- endset -%}
    {% do run_query(drop_query) %}
{%- endmacro -%}

{%- macro postgres__drop_table_by_name(table_name) -%}
    {%- set drop_query -%}
        DROP TABLE IF EXISTS {{ target.schema }}.{{ table_name }} CASCADE
    {%- endset -%}
    {% do run_query(drop_query) %}
{%- endmacro -%}
