"""
## Seed DAG

This DAG is used to illustrate setting an upstream dependency from the dbt DAGs. Notice the `outlets` parameter on the
`DbtSeedOperator` objects are creating
[Datasets](https://airflow.apache.org/docs/apache-airflow/stable/concepts/datasets.html).

We're using the dbt seed command here to populate the database for the purpose of this demo. Normally an extract DAG
would be ingesting data from various sources (i.e. sftp, blob like s3 or gcs, http endpoint, database, etc.)

"""

import os
from pathlib import Path

from airflow import DAG
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup
from pendulum import datetime

from cosmos import ProfileConfig
from cosmos.operators import DbtRunOperationOperator, DbtSeedOperator
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
    dag_id="extract_dag",
    start_date=datetime(2022, 11, 27),
    schedule="@daily",
    doc_md=__doc__,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "01-EXTRACT", "retries": 2},
) as dag:
    with TaskGroup(group_id="drop_seeds_if_exist") as drop_seeds:
        for seed in ["raw_customers", "raw_payments", "raw_orders"]:
            DbtRunOperationOperator(
                task_id=f"drop_{seed}_if_exists",
                macro_name="drop_table_by_name",
                args={"table_name": seed},
                project_dir=DBT_ROOT_PATH / "jaffle_shop",
                profile_config=profile_config,
                install_deps=True,
            )

    jaffle_shop_seed = DbtSeedOperator(
        task_id="seed_jaffle_shop",
        project_dir=DBT_ROOT_PATH / "jaffle_shop",
        outlets=[Dataset("SEED://JAFFLE_SHOP")],
        profile_config=profile_config,
        install_deps=True,
    )

    drop_seeds >> jaffle_shop_seed
