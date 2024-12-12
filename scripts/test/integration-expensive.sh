export SOURCE_RENDERING_BEHAVIOR=all
pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    -m integration  \
    --ignore=tests/perf \
    --ignore=tests/test_example_k8s_dags.py \
    -k 'example_cosmos_python_models or example_virtualenv'
