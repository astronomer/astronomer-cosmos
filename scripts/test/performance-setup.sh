pip uninstall -y dbt-core dbt-sqlite dbt-postgres openlineage-airflow openlineage-integration-common; \
rm -rf airflow.*; \
airflow db init; \
pip install 'dbt-postgres'
