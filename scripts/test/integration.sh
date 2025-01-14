#!/bin/bash

set -x
set -e

export SOURCE_RENDERING_BEHAVIOR=all

pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME

airflow db check


rm -rf dbt/jaffle_shop/dbt_packages;
pytest -vv tests/test_converter.py::test_converter_creates_dag_with_test_with_multiple_parents
