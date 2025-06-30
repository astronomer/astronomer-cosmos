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
    -m 'integration and not dbtfusion' \
    tests/test_example_k8s_dags.py
