-- Combine data from model_a and model_b
WITH model_a AS (
    SELECT * FROM {{ ref('model_a') }}
),
model_b AS (
    SELECT * FROM {{ ref('model_b') }}
)
SELECT
    a.id,
    a.name,
    b.created_at
FROM
    model_a AS a
JOIN
    model_b AS b
    ON a.id = b.id
