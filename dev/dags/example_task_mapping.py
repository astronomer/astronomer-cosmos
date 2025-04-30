import os
from datetime import datetime
from pathlib import Path

from airflow import DAG

from cosmos.config import ProfileConfig
from cosmos.operators.local import DbtRunLocalOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)

# Define the DAG
with DAG(
    dag_id="example_task_mapping",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    dbt_partial = DbtRunLocalOperator.partial(
        task_id="dbt_run", project_dir=DBT_ROOT_PATH / "simple", profile_config=profile_config, emit_datasets=False
    )

    dbt_run = dbt_partial.expand(select=["example_model"])  # Only run the specific model

    dbt_run
