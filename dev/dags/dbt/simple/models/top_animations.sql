
-- Use the `ref` function to select from other models

select Title, Rating
from {{ ref('movies_ratings_simplified') }}
WHERE Genre1=='Animation'
ORDER BY Rating desc
LIMIT 5;
