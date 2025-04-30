#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
echo "DBT_VERSION:"
echo "$DBT_VERSION"

# Calculate next minor version
NEXT_MINOR_VERSION=$(echo "$DBT_VERSION" | awk -F. '{print $1"."$2+1}')

echo "Installing dbt adapters for DBT_VERSION=$DBT_VERSION (<$NEXT_MINOR_VERSION)"

pip uninstall dbt-adapters dbt-common dbt-core dbt-extractor dbt-postgres dbt-semantic-interfaces -y
pip install -U \
  "dbt-postgres>=$DBT_VERSION,<$NEXT_MINOR_VERSION" \
  "dbt-databricks>=$DBT_VERSION,<$NEXT_MINOR_VERSION" \
  "dbt-bigquery>=$DBT_VERSION,<$NEXT_MINOR_VERSION"

# apache-airflow-core 3.0.0 requires pydantic>=2.11.0, but the above dbt adapters in case of version 1.6 and 1.9 install
# pydantic 1.10.22 which make it incompatible.
# With pydantic 1.10.22 we get the below error
# File "<...>/hatch/env/virtual/astronomer-cosmos/D9FI7Men/tests.py3.11-3.0-1.6/lib/python3.11/site-packages/cadwyn/_utils.py", line 5, in <module>
#     from pydantic._internal._decorators import unwrap_wrapped_function
# ModuleNotFoundError: No module named 'pydantic._internal'
# Hence, we re-install pydantic with the required minimum version after installing dbt adapters.
if [ "$DBT_VERSION" = "1.6" ] || [ "$DBT_VERSION" = "1.9" ]; then
    echo "DBT_VERSION is $DBT_VERSION, installing pydantic>=2.11.0 for apache-airflow-core compatibility."
    pip install "pydantic>2.11.0"
fi

# As on 28th April, 2025, the latest patch dbt-bigquery 1.6.13 on the 1.6 minor is not yet compatible with the latest
# release of google-cloud-bigquery 3.31.0 as it tries to access a protected attribute which no longer exists in that
# latest version for google-cloud-bigquery and gives the below error:
# stderr: module 'google.cloud.bigquery._helpers' has no attribute '_CELLDATA_FROM_JSON'.
# Hence, we need to install the previous version of google-cloud-bigquery <3.31.0 that still has the protected attribute
# 'google.cloud.bigquery._helpers._CELLDATA_FROM_JSON' available to make it compatible with dbt-bigquery 1.6.13.
if [ "$DBT_VERSION" = "1.6" ]; then
    echo "DBT_VERSION is $DBT_VERSION, installing google-cloud-bigquery<3.31.0 for compatibility issue."
    pip install "google-cloud-bigquery<3.31.0"
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

rm -rf dbt/jaffle_shop/dbt_packages
pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    "tests/test_async_example_dag.py::test_example_dag[simple_dag_async]"
