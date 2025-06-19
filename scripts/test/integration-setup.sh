#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
NEXT_MINOR_VERSION=$(echo "$DBT_VERSION" | awk -F. '{print $1"."$2+1}')

# we install using the following workaround to overcome installation conflicts, such as:
# apache-airflow 2.3.0 and dbt-core [0.13.0 - 1.5.2] and jinja2>=3.0.0 because these package versions have conflicting dependencies
pip uninstall -y 'dbt-bigquery' 'dbt-databricks' 'dbt-duckdb' 'dbt-postgres' 'dbt-vertica' 'dbt-core'
rm -rf airflow.*
pip freeze | grep airflow
airflow db reset -y

AIRFLOW_VERSION=$(airflow version)
AIRFLOW_MAJOR_VERSION=$(echo "$AIRFLOW_VERSION" | cut -d. -f1)
if [ "$AIRFLOW_MAJOR_VERSION" -ge 3 ]; then
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
    airflow db migrate
else
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db init'..."
    airflow db init
fi

uv pip install -U "dbt-core==$DBT_VERSION" dbt-postgres dbt-bigquery dbt-vertica 'dbt-databricks' pyspark

pip install  -U openlineage-airflow

if python3 -c "import sys; print(sys.version_info >= (3, 9))" | grep -q 'True'; then
  pip install  'dbt-duckdb' 'airflow-provider-duckdb>=0.2.0'
fi

# apache-airflow-core 3.0.0 requires pydantic>=2.11.0, but the above dbt adapters in case of version 1.6 and 1.9 install
# pydantic 1.10.22 which make it incompatible.
# With pydantic 1.10.22 we get the below error
# File "<...>/hatch/env/virtual/astronomer-cosmos/D9FI7Men/tests.py3.11-3.0-1.6/lib/python3.11/site-packages/cadwyn/_utils.py", line 5, in <module>
#     from pydantic._internal._decorators import unwrap_wrapped_function
# ModuleNotFoundError: No module named 'pydantic._internal'
# Hence, we re-install pydantic with the required minimum version after installing dbt adapters.
# dbt-core 1.9 raises
if [ "$DBT_VERSION" = "1.6" ] || [ "$DBT_VERSION" = "1.9" ]; then
    echo "DBT_VERSION is $DBT_VERSION, installing pydantic==2.11.0 for apache-airflow-core compatibility."
    pip install "pydantic==2.11.0"
fi
