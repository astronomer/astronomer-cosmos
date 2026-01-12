"""Example DAG for dbt Loom PoC - Finance Project (Downstream)
This DAG demonstrates using dbt Loom to reference public models from
the platform_project. dbt Loom will inject platform models during parsing,
allowing this project to reference them without executing them.
Key Test Points:
- Cosmos should parse the project successfully with dbt Loom enabled
- Finance models should show dependencies on platform models
- Cosmos should NOT try to execute platform models (they are injected metadata only)
- Task graph should only contain finance_project models
"""
import os
from datetime import datetime
from pathlib import Path
from cosmos import DbtDag, ProfileConfig, ProjectConfig
DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_PROJECT_PATH = DBT_ROOT_PATH / "finance_project"
profile_config = ProfileConfig(
    profile_name="finance_profile",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)
dbt_loom_finance_dag = DbtDag(
    project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
    profile_config=profile_config,
    operator_args={"install_deps": False},
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_loom_finance_dag",
    default_args={"retries": 0},
    doc_md=__doc__,
)
