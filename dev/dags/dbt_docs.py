"""
## Docs DAG

This DAG illustrates how to run `dbt docs generate` and handle the output. In this example, we're using the
`DbtDocsLocalOperator` to generate the docs, coupled with a callback. The callback will upload the docs to
S3 (if you have the S3Hook installed) or to a local directory.

"""

import os
from pathlib import Path

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
from airflow.decorators import task
from pendulum import datetime

from cosmos import ProfileConfig
from cosmos.operators import (
    DbtDocsAzureStorageOperator,
    DbtDocsS3Operator,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

S3_CONN_ID = "aws_docs"
AZURE_CONN_ID = "azure_docs"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db",
        profile_args={"schema": "public"},
    ),
)


@task.branch(task_id="which_upload")
def which_upload():
    "Only run the docs tasks if we have the proper connections set up"
    downstream_tasks_to_run = []

    try:
        BaseHook.get_connection(S3_CONN_ID)
        downstream_tasks_to_run += ["generate_dbt_docs_aws"]
    except AirflowNotFoundException:
        pass

    # if we have an AZURE_CONN_ID, check if it's valid
    try:
        BaseHook.get_connection(AZURE_CONN_ID)
        downstream_tasks_to_run += ["generate_dbt_docs_azure"]
    except AirflowNotFoundException:
        pass

    return downstream_tasks_to_run


with DAG(
    dag_id="docs_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    doc_md=__doc__,
    catchup=False,
) as dag:
    generate_dbt_docs_aws = DbtDocsS3Operator(
        task_id="generate_dbt_docs_aws",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        profile_config=profile_config,
        aws_conn_id=S3_CONN_ID,
        bucket_name="cosmos-docs",
    )

    generate_dbt_docs_azure = DbtDocsAzureStorageOperator(
        task_id="generate_dbt_docs_azure",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        profile_config=profile_config,
        azure_conn_id=AZURE_CONN_ID,
        container_name="$web",
    )

    which_upload() >> [generate_dbt_docs_aws, generate_dbt_docs_azure]
