#!/bin/bash

set -v
set -x
set -e

# we install using the following workaround to overcome installation conflicts, such as:
# apache-airflow 2.3.0 and dbt-core [0.13.0 - 1.5.2] and jinja2>=3.0.0 because these package versions have conflicting dependencies
pip uninstall -y 'dbt-bigquery' 'dbt-databricks' 'dbt-postgres' 'dbt-vertica' 'dbt-core'
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

pip install 'dbt-databricks!=1.9.0' 'dbt-bigquery' 'dbt-postgres' 'dbt-vertica' 'openlineage-airflow'
