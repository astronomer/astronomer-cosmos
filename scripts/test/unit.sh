pytest \
    -vv \
    -m "not (integration or perf)" \
    --ignore=tests/perf \
    --ignore=tests/test_example_dags.py \
    --ignore=tests/test_async_example_dag.py \
    --ignore=tests/test_example_dags_no_connections.py \
    --ignore=tests/test_example_k8s_dags.py
