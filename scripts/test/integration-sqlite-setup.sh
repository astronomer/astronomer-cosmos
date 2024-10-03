pip uninstall -y dbt-core dbt-sqlite openlineage-airflow openlineage-integration-common; \
rm -rf airflow.*; \
airflow db init; \
pip install 'dbt-core==1.5' 'dbt-sqlite<=1.5' 'dbt-databricks<=1.5' 'dbt-postgres<=1.5' 'dbt-bigquery<=1.5'
