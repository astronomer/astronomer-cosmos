{%- macro to_unixtimestamp(timestamp) -%}
    {{ adapter.dispatch("to_unixtimestamp", "dbt_date")(timestamp) }}
{%- endmacro %}

{%- macro default__to_unixtimestamp(timestamp) -%}
    {{ dbt_date.date_part("epoch", timestamp) }}
{%- endmacro %}

{%- macro snowflake__to_unixtimestamp(timestamp) -%}
    {{ dbt_date.date_part("epoch_seconds", timestamp) }}
{%- endmacro %}

{%- macro bigquery__to_unixtimestamp(timestamp) -%}
    unix_seconds({{ timestamp }})
{%- endmacro %}

{%- macro spark__to_unixtimestamp(timestamp) -%}
    unix_timestamp({{ timestamp }})
{%- endmacro %}

{%- macro trino__to_unixtimestamp(timestamp) -%}
    to_unixtime({{ timestamp }} at time zone 'UTC')
{%- endmacro %}
