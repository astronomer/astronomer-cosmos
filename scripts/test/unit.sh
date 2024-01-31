pytest \
    -vv \
    --durations=0 \
    -m "not integration" \
    --ignore=tests/test_example_dags.py \
    --ignore=tests/test_example_dags_no_connections.py