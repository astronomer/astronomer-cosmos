#!/bin/bash

set -x
set -e

export SOURCE_RENDERING_BEHAVIOR=all

pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME

airflow db check

pytest -vv \
    tests/dbt/test_runner.py::test_run_command \
    -m 'integration and not dbtfusion'
