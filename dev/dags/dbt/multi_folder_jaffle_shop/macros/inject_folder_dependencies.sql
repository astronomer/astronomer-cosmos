{% macro inject_folder_dependencies() %}
  {# Current folder key from path, e.g. "models/staging/stg_customers.sql" -> "models/staging" #}
  {% set path_parts = model.original_file_path.split('/') %}
  {% set folder_key = path_parts[0] ~ '/' ~ path_parts[1] %}

  {# Which folders this folder depends on (e.g. models/staging -> [seeds], models/production -> [models/staging]) #}
  {% set folder_deps = var('folder_dependencies', {}).get(folder_key, []) %}
  {# Node names per folder (seeds, models/staging, models/production) #}
  {% set dbt_nodes_by_folder = var('dbt_nodes_by_folder', {}) %}

  {% set dep_refs = [] %}
  {% for dep_folder in folder_deps %}
    {% for node_name in dbt_nodes_by_folder.get(dep_folder, []) %}
      {% do dep_refs.append(node_name) %}
    {% endfor %}
  {% endfor %}

  {# Return SQL that includes ref() calls so dbt builds the DAG; no-op so it doesn't change results #}
  {% if dep_refs | length > 0 %}
SELECT 1 FROM (
  {% for r in dep_refs %}
  SELECT 1 AS _one FROM {{ ref(r) }} WHERE 1=0{% if not loop.last %} UNION ALL {% endif %}
  {% endfor %}
) _ WHERE 1=0
  {% else %}
SELECT 1 WHERE 1=0
  {% endif %}
{% endmacro %}
