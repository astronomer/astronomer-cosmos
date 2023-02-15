"""
## Jaffle Shop DAG
[Jaffle Shop](https://github.com/dbt-labs/jaffle_shop) is a fictional eCommerce store. This dbt project originates from
dbt labs as an example project with dummy data to demonstrate a working dbt core project. This DAG uses the cosmos dbt
parser to generate an Airflow TaskGroup from the dbt project folder

"""
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

from cosmos.providers.dbt.task_group import DbtTaskGroup

with DAG(
    dag_id="jaffle_shop",
    start_date=datetime(2022, 11, 27),
    schedule=[Dataset("SEED://JAFFLE_SHOP")],
    doc_md=__doc__,
    catchup=False,
    default_args={"owner": "02-TRANSFORM"},
) as dag:
    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    jaffle_shop = DbtTaskGroup(
        dbt_project_name="jaffle_shop",
        conn_id="airflow_db",
        dbt_args={
            "schema": "public",
            "dbt_executable_path": "/usr/local/airflow/dbt_venv/bin/dbt",
        },
        test_behavior="after_all",
    )

    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow")

    pre_dbt_workflow >> jaffle_shop >> post_dbt_workflow
