"""
## Docs DAG

This DAG illustrates how to run `dbt docs generate` and handle the output. In this example, we're using the
`DbtDocsLocalOperator` to generate the docs, coupled with a callback. The callback will upload the docs to
S3 (if you have the S3Hook installed) or to a local directory.

"""

import os
from pathlib import Path

from airflow import DAG
from pendulum import datetime

from cosmos.providers.dbt.core.operators import (
    DbtDocsAzureStorageOperator,
    DbtDocsS3Operator,
)

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

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
        conn_id="airflow_db",
        schema="public",
        aws_conn_id="aws_default",
        bucket_name="cosmos-docs",
    )

    generate_dbt_docs_azure = DbtDocsAzureStorageOperator(
        task_id="generate_dbt_docs_azure",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        conn_id="airflow_db",
        schema="public",
        azure_conn_id="test_azure",
        container_name="$web",
    )

    [generate_dbt_docs_aws, generate_dbt_docs_azure]
