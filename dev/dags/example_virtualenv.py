"""
An example DAG that uses Cosmos to render a dbt project.
"""
import os
from datetime import datetime
from pathlib import Path

from cosmos.dag import DbtDag

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROJECT_NAME = "jaffle_shop"
CONNECTION_ID = "airflow_db"

# [START virtualenv_example]
example_virtualenv = DbtDag(
    # dbt/cosmos-specific parameters
    dbt_root_path=DBT_ROOT_PATH,
    dbt_project_name=PROJECT_NAME,
    conn_id=CONNECTION_ID,
    dbt_args={"schema": "public"},
    execution_mode="virtualenv",
    operator_args={
        "project_dir": DBT_ROOT_PATH / PROJECT_NAME,
        "py_system_site_packages": False,
        "py_requirements": ["dbt-postgres==1.6.0b1"],
    },
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_virtualenv",
)
# [END virtualenv_example]
