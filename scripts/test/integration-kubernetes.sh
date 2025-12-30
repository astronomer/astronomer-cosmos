#!/bin/bash

set -x
set -e

# So we can validate ExecutionMode.WATCHER_KUBERNETES
actual_version=$(airflow version | cut -d. -f1)
if [ "$actual_version" = "3" ] ; then
    pip install "apache-airflow-providers-cncf-kubernetes==10.8.0" "protobuf==6.33.2"

fi

# Reset the Airflow database to its initial state
airflow db reset -y

# Run tests using pytest
pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    -m 'integration and not dbtfusion' \
    tests/test_example_k8s_dags.py \
    tests/operators/test_watcher_kubernetes.py
