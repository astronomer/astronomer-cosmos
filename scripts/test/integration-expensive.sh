export SOURCE_RENDERING_BEHAVIOR=all

# These DAGs run on Postgres, while most example DAGs now use BigQuery, which this job does not configure. Run each in
# its own invocation with TEST_SINGLE_DAG so that only that DAG is parsed (see get_dag_bag_single_dag in
# tests/test_example_dags.py), and skip test_example_dags_no_connections.py, which parses every example DAG at collection.
export TEST_SINGLE_DAG="example_cosmos_python_models.py"
pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    -m 'integration and not dbtfusion' \
    --ignore=tests/perf \
    --ignore=tests/test_example_k8s_dags.py \
    --ignore=tests/test_example_dags_no_connections.py \
    -k 'example_cosmos_python_models'

export TEST_SINGLE_DAG="example_virtualenv.py"
pytest -vv \
    --cov=cosmos \
    --cov-append \
    --cov-report=term-missing \
    --cov-report=xml \
    -m 'integration and not dbtfusion' \
    --ignore=tests/perf \
    --ignore=tests/test_example_k8s_dags.py \
    --ignore=tests/test_example_dags_no_connections.py \
    -k 'example_virtualenv'
