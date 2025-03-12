#!/bin/bash

set -x
set -e

DBT_VERSION="$1"
echo "DBT_VERSION:"
echo "$DBT_VERSION"


pip uninstall dbt-adapters dbt-common dbt-core dbt-extractor dbt-postgres dbt-semantic-interfaces -y
pip install "dbt-postgres==$DBT_VERSION"  "dbt-databricks==$DBT_VERSION" "dbt-bigquery==$DBT_VERSION"
export SOURCE_RENDERING_BEHAVIOR=all
rm -rf airflow.*; \
airflow db init; \


rm -rf dbt/jaffle_shop/dbt_packages;
pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    "tests/test_async_example_dag.py::test_example_dag[simple_dag_async]"
