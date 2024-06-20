"""
## Docs DAG

This DAG illustrates how to run `dbt docs generate` and handle the output. In this example, we're using the
`DbtDocsLocalOperator` to generate the docs, coupled with a callback. The callback will upload the docs to
S3 (if you have the S3Hook installed) or to a local directory.

"""

import os
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from pendulum import datetime

from cosmos import ProfileConfig
from cosmos.operators import (
    DbtDocsAzureStorageOperator,
    DbtDocsGCSOperator,
    DbtDocsS3Operator,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

S3_CONN_ID = "aws_docs"
AZURE_CONN_ID = "azure_docs"
GCS_CONN_ID = "gcs_docs"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
    ),
)


@task.branch(task_id="which_upload")
def which_upload():
    """Only run the docs tasks if we have the proper connections set up"""
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
    try:
        BaseHook.get_connection(GCS_CONN_ID)
        downstream_tasks_to_run += ["generate_dbt_docs_gcs"]
    except AirflowNotFoundException:
        pass

    return downstream_tasks_to_run


with DAG(
    dag_id="docs_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    doc_md=__doc__,
    catchup=False,
    default_args={"retries": 2},
) as dag:
    generate_dbt_docs_aws = DbtDocsS3Operator(
        task_id="generate_dbt_docs_aws",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        profile_config=profile_config,
        connection_id=S3_CONN_ID,
        bucket_name="cosmos-docs",
    )

    generate_dbt_docs_azure = DbtDocsAzureStorageOperator(
        task_id="generate_dbt_docs_azure",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        profile_config=profile_config,
        connection_id=AZURE_CONN_ID,
        bucket_name="$web",
    )

    generate_dbt_docs_gcs = DbtDocsGCSOperator(
        task_id="generate_dbt_docs_gcs",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        profile_config=profile_config,
        connection_id=GCS_CONN_ID,
        bucket_name="cosmos-docs",
    )

    which_upload() >> [generate_dbt_docs_aws, generate_dbt_docs_azure, generate_dbt_docs_gcs]
