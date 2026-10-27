{# Fails the first time it runs for a given name (and COSMOS_FAIL_ONCE_TOKEN) and passes afterwards.
   The number of runs is kept in a table of the target schema, so it survives across dbt invocations (a producer retry,
   a consumer fallback) on any adapter. The statements use auto_begin and the commit comes before the failure because
   the hook runs in a transaction that the failure rolls back, and the count has to outlive it. #}
{% macro fail_once(name) %}
    {% if execute %}
        {% set relation = api.Relation.create(
            database=target.database,
            schema=target.schema,
            identifier=name ~ '_' ~ env_var('COSMOS_FAIL_ONCE_TOKEN', '')
        ) %}
        {% call statement('fail_once_create', auto_begin=True) %}create table if not exists {{ relation }} (n integer){% endcall %}
        {% call statement('fail_once_insert', auto_begin=True) %}insert into {{ relation }} values (1){% endcall %}
        {% call statement('fail_once_runs', fetch_result=True, auto_begin=True) %}select count(*) from {{ relation }}{% endcall %}
        {% set runs = load_result('fail_once_runs')['data'][0][0] %}
        {% do adapter.commit() %}
        {% if runs <= 1 %}
            {{ exceptions.raise_compiler_error('fail_once: intentional first-run failure') }}
        {% endif %}
    {% endif %}
    select 1
{% endmacro %}
