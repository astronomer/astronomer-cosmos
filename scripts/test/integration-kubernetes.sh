#!/bin/bash

set -x
set -e

# export POSTGRES_DB=postgres

airflow db reset -y

pytest tests/test_example_k8s_dags.py
