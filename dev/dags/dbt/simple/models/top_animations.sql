{{ config(materialized='table') }}

SELECT Title, Rating
FROM {{ ref('movies_ratings_simplified') }}
WHERE Genre1=='Animation'
ORDER BY Rating desc
LIMIT 5;
