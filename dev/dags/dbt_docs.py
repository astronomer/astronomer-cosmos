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

from cosmos import ProfileConfig
from cosmos.operators import (
    DbtDocsAzureStorageOperator,
    DbtDocsGCSOperator,
    DbtDocsS3Operator,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
    ),
)


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
        connection_id="aws_s3_conn",
        bucket_name="cosmos-ci-docs",
        install_deps=True,
    )

    generate_dbt_docs_azure = DbtDocsAzureStorageOperator(
        task_id="generate_dbt_docs_azure",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        profile_config=profile_config,
        connection_id="azure_wasb_conn",
        bucket_name="cosmos-ci-docs",
        install_deps=True,
    )

    generate_dbt_docs_gcs = DbtDocsGCSOperator(
        task_id="generate_dbt_docs_gcs",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        profile_config=profile_config,
        connection_id="gcp_gs_conn",
        bucket_name="cosmos-ci-docs",
        install_deps=True,
    )
