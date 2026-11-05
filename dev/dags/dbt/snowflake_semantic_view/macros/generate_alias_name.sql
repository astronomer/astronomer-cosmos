{% macro generate_alias_name(custom_alias_name=None, node=None) -%}
  {%- set base = custom_alias_name if custom_alias_name else node.name -%}
  {%- if env_var('RESOURCE_PREFIX', '') -%}
    {{ return(env_var('RESOURCE_PREFIX') ~ '_' ~ base) }}
  {%- else -%}
    {{ base }}
  {%- endif -%}
{%- endmacro %}
