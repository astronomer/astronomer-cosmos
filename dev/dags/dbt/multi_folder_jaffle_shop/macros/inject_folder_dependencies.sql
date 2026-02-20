{% macro inject_folder_dependencies() %}
  {# Extract folder name from path, e.g. "models/production/customers.sql" -> "production" #}
  {% set path_parts = model.original_file_path.split('/') %}
  {% set folder_name = path_parts[-2] %}

  {# Which folders this folder depends on (e.g. production -> [staging]) #}
  {% set folder_deps = var('folder_dependencies', {}).get(folder_name, []) %}
  {# Model names per folder (from vars; avoids using graph which can be dict vs object across dbt versions) #}
  {% set folder_models = var('folder_models', {}) %}

  {% set dep_refs = [] %}
  {% for dep_folder in folder_deps %}
    {% for m in folder_models.get(dep_folder, []) %}
      {% do dep_refs.append(m) %}
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
