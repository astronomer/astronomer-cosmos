#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
echo "DBT_VERSION:"
echo "$DBT_VERSION"


pip uninstall dbt-adapters dbt-common dbt-core dbt-extractor dbt-postgres dbt-semantic-interfaces -y
pip install "dbt-postgres==$DBT_VERSION"  "dbt-databricks==$DBT_VERSION" "dbt-bigquery==$DBT_VERSION"
export SOURCE_RENDERING_BEHAVIOR=all
rm -rf airflow.*

AIRFLOW_VERSION=$(airflow version)
AIRFLOW_MAJOR_VERSION=$(echo "$AIRFLOW_VERSION" | cut -d. -f1)
if [ "$AIRFLOW_MAJOR_VERSION" -ge 3 ]; then
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
    airflow db migrate
else
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db init'..."
    airflow db init
fi

if [ "$DBT_VERSION" = "1.7" ]; then
    # Otherwise, we will get the following error:
    # stderr: MessageToJson() got an unexpected keyword argument 'including_default_value_fields'
    echo "DBT version is 1.7 — Installing protobuf==4.25.6..."
    pip install protobuf==4.25.6
fi

rm -rf dbt/jaffle_shop/dbt_packages
pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    "tests/test_async_example_dag.py::test_example_dag[simple_dag_async]"
