{% macro date(year, month, day) %}
    {{ return(modules.datetime.date(year, month, day)) }}
{% endmacro %}

{% macro datetime(
    year, month, day, hour=0, minute=0, second=0, microsecond=0, tz=None
) %}
    {% set tz = tz if tz else var("dbt_date:time_zone") %}
    {{
        return(
            modules.datetime.datetime(
                year=year,
                month=month,
                day=day,
                hour=hour,
                minute=minute,
                second=second,
                microsecond=microsecond,
                tzinfo=modules.pytz.timezone(tz),
            )
        )
    }}
{% endmacro %}
