#!/bin/bash

set -x
set -e

# Reset the Airflow database to its initial state
airflow db reset -y

# Run tests using pytest
pytest tests/test_example_k8s_dags.py
