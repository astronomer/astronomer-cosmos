{{ config(materialized='table') }}

select
  "Title",
  "Rating",
  "TotalVotes",
  "Genre1",
  "Genre2",
  "Genre3",
  "Budget",
  "Domestic",
  "Foreign",
  "Worldwide"
from {{ source('imdb', 'movies_ratings') }}
