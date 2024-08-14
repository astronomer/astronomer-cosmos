pip uninstall dbt-adapters dbt-common dbt-core dbt-extractor dbt-postgres dbt-semantic-interfaces -y
pip install dbt-postgres==1.5.4  dbt-databricks==1.5.4
export SOURCE_RENDERING_BEHAVIOR=all
rm -rf airflow.*; \
airflow db init; \
pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    -m integration  \
    --ignore=tests/perf \
    -k 'basic_cosmos_task_group'
