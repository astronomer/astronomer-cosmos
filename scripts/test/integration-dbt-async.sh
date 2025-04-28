#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
echo "DBT_VERSION:"
echo "$DBT_VERSION"


pip uninstall dbt-adapters dbt-common dbt-core dbt-extractor dbt-postgres dbt-semantic-interfaces -y
pip install "dbt-postgres==$DBT_VERSION"  "dbt-databricks==$DBT_VERSION" "dbt-bigquery==$DBT_VERSION"

# apache-airflow-core 3.0.0 requires pydantic>=2.11.0, but the above dbt adapters in case of version 1.6 and 1.9 install
# pydantic 1.10.22 which make it incompatible.
# With pydantic 1.10.22 we get the below error
# File "<...>/hatch/env/virtual/astronomer-cosmos/D9FI7Men/tests.py3.11-3.0-1.6/lib/python3.11/site-packages/cadwyn/_utils.py", line 5, in <module>
#     from pydantic._internal._decorators import unwrap_wrapped_function
# ModuleNotFoundError: No module named 'pydantic._internal'
# Hence, we re-install pydantic with the required minimum version after installing dbt adapters.
if [[ "$DBT_VERSION" == "1.6" || "$DBT_VERSION" == "1.9" ]]; then
    echo "DBT_VERSION is $DBT_VERSION, installing pydantic>=2.11.0 for apache-airflow-core compatibility."
    pip install "pydantic>2.11.0"
fi

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
