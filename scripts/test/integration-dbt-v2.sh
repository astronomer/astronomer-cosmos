# Run the dbt v2 (dbt-oss) integration tests. They run against BigQuery and are skipped unless COSMOS_TEST_DBT_V2=1.
pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    --timeout=600 \
    --timeout-method=thread \
    "$@" \
    tests/dbt_v2
