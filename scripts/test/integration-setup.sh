#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
NEXT_MINOR_VERSION=$(echo "$DBT_VERSION" | awk -F. '{print $1"."$2+1}')

# we install using the following workaround to overcome installation conflicts, such as:
# apache-airflow 2.3.0 and dbt-core [0.13.0 - 1.5.2] and jinja2>=3.0.0 because these package versions have conflicting dependencies
pip uninstall -y 'dbt-bigquery' 'dbt-duckdb' 'dbt-postgres' 'dbt-vertica' 'dbt-core'
pip install -U 'dbt-adapters>=1.16' 'dbt-databricks'

rm -f $AIRFLOW_HOME/airflow.cfg
rm -f $AIRFLOW_HOME/airflow.db

pip freeze | grep airflow
airflow db reset -y


AIRFLOW_VERSION=$(airflow version)
AIRFLOW_MAJOR_VERSION=$(echo "$AIRFLOW_VERSION" | cut -d. -f1)
if [ "$AIRFLOW_MAJOR_VERSION" -ge 3 ]; then
  # https://github.com/zmievsa/cadwyn/issues/283, hence kept cadwyn>=5.4.1
  # https://github.com/zmievsa/cadwyn/issues/305, hence kept fastapi<0.121.0
    uv pip install "cadwyn>=5.4.1" "fastapi<0.121.0"
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
    airflow db migrate
else
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db init'..."
    airflow db init
fi

uv pip install -U "dbt-core~=$DBT_VERSION" dbt-postgres dbt-bigquery dbt-vertica dbt-databricks pyspark

if python3 -c "import sys; print(sys.version_info >= (3, 10))" | grep -q 'True'; then
  pip install 'dbt-duckdb' "airflow-provider-duckdb>=0.2.0"
fi

pip install -U openlineage-airflow apache-airflow-providers-google apache-airflow==$AIRFLOW_VERSION

if [ "$AIRFLOW_VERSION" = "3.1.0" ] ; then
  # This error was happening only in Airflow 3.1:
  # No module named 'dbt.adapters.catalogs'
  # So we are overcoming this with:
  pip install "dbt-adapters>1.14.3,<2.0"
fi


uv pip freeze
