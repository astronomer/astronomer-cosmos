#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
NEXT_MINOR_VERSION=$(echo "$DBT_VERSION" | awk -F. '{print $1"."$2+1}')

# we install using the following workaround to overcome installation conflicts, such as:
# apache-airflow 2.3.0 and dbt-core [0.13.0 - 1.5.2] and jinja2>=3.0.0 because these package versions have conflicting dependencies
rm -f $AIRFLOW_HOME/airflow.cfg
rm -f $AIRFLOW_HOME/airflow.db

airflow db reset -y

# ATTENTION: Airflow and its dependencies are installed using pre-install-airflow.sh script, so we don't install them here.
AIRFLOW_VERSION=$(airflow version)
AIRFLOW_MAJOR_VERSION=$(echo "$AIRFLOW_VERSION" | cut -d. -f1)
if [ "$AIRFLOW_MAJOR_VERSION" -ge 3 ]; then
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
    airflow db migrate
else
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db init'..."
    airflow db init
fi

uv pip freeze
