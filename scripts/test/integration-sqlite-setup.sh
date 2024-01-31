pip uninstall -y dbt-core dbt-sqlite openlineage-airflow openlineage-integration-common; \
rm -rf airflow.*; \
airflow db init; \
pip install 'dbt-core==1.4' 'dbt-sqlite<=1.4' 'dbt-databricks<=1.4' 'dbt-postgres<=1.4'
