{{
  config(
    tags=["flaky"],
    materialized="table",
    pre_hook=[
      "CREATE TABLE IF NOT EXISTS {{ target.schema }}.retry_flag (attempt integer)",
      "INSERT INTO {{ target.schema }}.retry_flag (attempt) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM {{ target.schema }}.retry_flag)",
      "UPDATE {{ target.schema }}.retry_flag SET attempt = attempt + 1"
    ]
  )
}}

-- Simulates a transient failure for testing dbt retry.
-- Fully self-contained: no manual DB setup required.
--
-- Pre-hooks run on every attempt (including dbt retry):
--   1. Create flag table if not exists
--   2. Seed with 0 if empty
--   3. Increment attempt counter
--
-- First dbt build: pre-hooks set attempt=1, SELECT hits 1/0, model fails.
-- dbt retry: pre-hooks set attempt=2, SELECT returns 1, model succeeds.

SELECT
    CASE
        WHEN (SELECT attempt FROM {{ target.schema }}.retry_flag LIMIT 1) <= 1
        THEN 1 / 0  -- Fail on first attempt
        ELSE 1
    END AS result
