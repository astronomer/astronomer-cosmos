#!/bin/bash

set -x
set -e

# Reset the Airflow database to its initial state
airflow db reset -y

# Run tests using pytest
pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    -m integration \
    ../tests/test_example_k8s_dags.py
