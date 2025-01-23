import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ExecutionMode, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="gcp_gs_conn", profile_args={"dataset": "release_17", "project": "astronomer-dag-authoring"}
    ),
)


# [START airflow_async_execution_mode_example]
simple_dag_async = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "original_jaffle_shop",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.AIRFLOW_ASYNC,
    ),
    render_config=RenderConfig(
        select=["path:models"],
        # test_behavior=TestBehavior.NONE
    ),
    # normal dag parameters
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="simple_dag_async",
    tags=["simple"],
    operator_args={"async_args": {"full_refresh": True, "location": "northamerica-northeast1"}},
)
# [END airflow_async_execution_mode_example]
