pytest -vv \
    -s \
    tests/perf/test_performance.py \
    --ignore=tests/test_example_dags.py \
    --ignore=tests/test_async_example_dag.py \
    --ignore=tests/test_example_dags_no_connections.py \
    --ignore=tests/test_example_k8s_dags.py
