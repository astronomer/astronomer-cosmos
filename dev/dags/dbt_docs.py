"""
## Docs DAG

This DAG illustrates how to run `dbt docs generate` and handle the output. In this example, we're using the
`DbtDocsLocalOperator` to generate the docs, coupled with a callback. The callback will upload the docs to
S3 (if you have the S3Hook installed) or to a local directory.

"""

import os
import shutil

from airflow import DAG
from pendulum import datetime

from cosmos.providers.dbt.core.operators import DbtDocsOperator


def docs_callback(project_dir: str) -> None:
    """
    Callback function to print the path to the generated docs.
    """
    target_dir = f"{project_dir}/target"

    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="aws_default")

        # iterate over the files in the target dir and upload them to S3
        for dirpath, _, filenames in os.walk(target_dir):
            for filename in filenames:
                hook.load_file(
                    filename=f"{dirpath}/{filename}",
                    bucket_name="my-bucket",
                    key=f"dbt-docs/{filename}",
                    replace=True,
                )

        return

    # if the S3Hook isn't installed, just copy the target dir to a local dir
    except ImportError:
        pass

    # if there's a botocore.exceptions.NoCredentialsError, print a warning and just copy the docs locally
    except Exception as exc:
        if "NoCredentialsError" in str(exc):
            print(
                "WARNING: No AWS credentials found.\
                To upload docs to S3, install the S3Hook and configure an S3 connection."
            )

    # copy the target dir to /usr/local/airflow/dbt-docs
    if os.path.exists("/usr/local/airflow/dbt-docs"):
        shutil.rmtree("/usr/local/airflow/dbt-docs")

    shutil.copytree(target_dir, "/usr/local/airflow/dbt-docs")


with DAG(
    dag_id="docs_dag",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    doc_md=__doc__,
    catchup=False,
) as dag:
    generate_dbt_docs = DbtDocsOperator(
        task_id="generate_dbt_docs",
        project_dir="/usr/local/airflow/dags/dbt/jaffle_shop",
        profile_args={
            "schema": "public",
        },
        conn_id="airflow_db",
        callback=docs_callback,
    )

    generate_dbt_docs
