#!/bin/bash

set -x
set -e


airflow db reset -y

pytest tests/test_example_k8s_dags.py
