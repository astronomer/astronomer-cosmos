#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
echo "DBT_VERSION:"
echo "$DBT_VERSION"
NEXT_MINOR_VERSION=$(echo "$DBT_VERSION" | awk -F. '{print $1"."$2+1}')

pip uninstall dbt-adapters dbt-common dbt-core dbt-extractor dbt-postgres dbt-semantic-interfaces -y
pip install -U \
  "dbt-core>=$DBT_VERSION,<$NEXT_MINOR_VERSION" dbt-postgres

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
