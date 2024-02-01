pytest -vv \
    -s \
    -m 'perf' \
    --ignore=tests/test_example_dags.py \
    --ignore=tests/test_example_dags_no_connections.py