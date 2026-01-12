"""Example DAG for dbt Loom PoC - Platform Project (Upstream)
This DAG represents the upstream project that contains public models
which will be referenced by the finance project via dbt Loom.
"""
import os
from datetime import datetime
from pathlib import Path
from cosmos import DbtDag, ProfileConfig, ProjectConfig
DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_PATH = DBT_ROOT_PATH / "platform_project"
profile_config = ProfileConfig(
    profile_name="platform_profile",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)
dbt_loom_platform_dag = DbtDag(
    project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
    profile_config=profile_config,
    operator_args={"install_deps": False},
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_loom_platform_dag",
    default_args={"retries": 0},
    doc_md=__doc__,
)
