import os
from datetime import datetime
from pathlib import Path

from airflow.models import DAG

from cosmos import ProfileConfig
from cosmos.operators.virtualenv import DbtSeedVirtualenvOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

DEFAULT_DBT_ROOT_PATH = Path(__file__).resolve().parent / "dbt"
DBT_PROJ_DIR = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH)) / "jaffle_shop"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
    ),
)

with DAG("example_virtualenv_mini", start_date=datetime(2022, 1, 1)) as dag:
    pre = BashOperator(task_id="pre", bash_command="mkdir -p /tmp/persistent-venv2")

    seed_operator = DbtSeedVirtualenvOperator(
        profile_config=profile_config,
        project_dir=DBT_PROJ_DIR,
        task_id="seed",
        dbt_cmd_flags=["--select", "raw_customers"],
        install_deps=True,
        append_env=True,
        py_system_site_packages=False,
        py_requirements=["dbt-postgres"],
        virtualenv_dir=Path("/tmp/persistent-venv2"),
    )
    pre >> seed_operator
