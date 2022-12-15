"""
## Jaffle Shop DAG
[Jaffle Shop](https://github.com/dbt-labs/jaffle_shop) is a fictional eCommerce store. This dbt project originates from
dbt labs as an example project with dummy data to demonstrate a working dbt core project. This DAG uses the dbt parser
stored in `/include/utils/dbt_dag_parser.py` to parse this project (from dbt's
[manifest.json](https://docs.getdbt.com/reference/artifacts/manifest-json) file) and dynamically create Airflow tasks
and dependencies.

"""
from pendulum import datetime

from airflow import DAG
from airflow.datasets import Dataset
from cosmos.providers.dbt.core.operators import DBTSeedOperator
from cosmos.providers.dbt.task_group import DbtTaskGroup

with DAG(
    dag_id="jaffle_shop",
    start_date=datetime(2022, 11, 27),
    schedule=[Dataset("DAG://EXTRACT_DAG")],
    doc_md=__doc__,
    catchup=False,
    default_args={
        "owner": "02-TRANSFORM"
    }
) as dag:

    seed = DBTSeedOperator(
        task_id="dbt_seed",
        project_dir="/usr/local/airflow/dbt/jaffle_shop",
        full_refresh=True,
        schema="public",
        conn_id="airflow_db",
        python_venv="/usr/local/airflow/dbt_venv/bin/activate"
    )

    jaffle_shop = DbtTaskGroup(
        dbt_project_name="jaffle_shop",
        conn_id="airflow_db",
        dbt_args={
            "schema": "public",
            "python_venv": "/usr/local/airflow/dbt_venv/bin/activate"
        },
        dag=dag
    )

    seed >> jaffle_shop


