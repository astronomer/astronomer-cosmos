#!/bin/bash

set -x

# we install using the following workaround to overcome installation conflicts, such as:
# apache-airflow 2.3.0 and dbt-core [0.13.0 - 1.5.2] and jinja2>=3.0.0 because these package versions have conflicting dependencies
pip uninstall -y dbt-postgres dbt-databricks dbt-vertica
rm -rf airflow.*
pip freeze | grep airflow
airflow db migrate
pip install 'dbt-core' 'dbt-databricks' 'dbt-postgres' 'dbt-vertica' 'openlineage-airflow'
