{{
  config(
    tags=["flaky"],
    materialized="table",
    post_hook=[
      "DELETE FROM {{ target.schema }}.retry_flag"
    ]
  )
}}

-- Depends on flaky_model so it runs after the retry succeeds.
-- Post-hook resets the flag table so the next DAG run starts fresh.

SELECT * FROM {{ ref('flaky_model') }}
