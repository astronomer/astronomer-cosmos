pytest \
    -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    -m "not integration" \
    --ignore=tests/test_example_dags.py \
    --ignore=tests/test_example_dags_no_connections.p