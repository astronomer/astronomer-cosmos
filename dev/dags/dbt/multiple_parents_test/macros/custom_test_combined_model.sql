{% test custom_test_combined_model(model) %}
WITH source_data AS (
    SELECT id FROM {{ ref('model_a') }}
),
combined_data AS (
    SELECT id FROM {{ model }}
)
SELECT
    s.id
FROM
    source_data s
LEFT JOIN
    combined_data c
    ON s.id = c.id
WHERE
    c.id IS NULL
{% endtest %}
