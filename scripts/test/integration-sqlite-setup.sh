pip uninstall -y dbt-core dbt-sqlite openlineage-airflow openlineage-integration-common; \
rm -rf airflow.*; \
airflow db init; \
pip install 'dbt-core<1.8' 'dbt-sqlite' 'dbt-databricks' 'dbt-postgres'
