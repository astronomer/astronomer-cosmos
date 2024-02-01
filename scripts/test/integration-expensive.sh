pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    -m integration  \
    --ignore=tests/perf \
    -k 'example_cosmos_python_models or example_virtualenv'
