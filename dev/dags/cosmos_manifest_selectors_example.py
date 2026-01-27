"""
An example DAG that uses Cosmos to render a dbt project into Airflow using a dbt manifest file with YAML selectors.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ExecutionConfig, LoadMode, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import DbtProfileConfigVars, PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

execution_config = ExecutionConfig(dbt_project_path=DBT_ROOT_PATH / "jaffle_shop")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="example_conn",
        profile_args={"schema": "public"},
        dbt_config_vars=DbtProfileConfigVars(send_anonymous_usage_stats=True),
    ),
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_MANIFEST,
    selector="critical_path",
    airflow_vars_to_purge_dbt_yaml_selectors_cache=["purge"],
)


with DAG(
    dag_id="cosmos_manifest_selectors_example",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"retries": 0},
):
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    # [START local_example]
    local_example = DbtTaskGroup(
        group_id="local_example",
        project_config=ProjectConfig(
            manifest_path=DBT_ROOT_PATH / "jaffle_shop" / "target" / "manifest.json",
            project_name="jaffle_shop",
        ),
        profile_config=profile_config,
        render_config=render_config,
        execution_config=execution_config,
        operator_args={"install_deps": True},
    )
    # [END local_example]

    # [START aws_s3_example]
    aws_s3_example = DbtTaskGroup(
        group_id="aws_s3_example",
        project_config=ProjectConfig(
            manifest_path="s3://cosmos-manifest-test/manifest.json",
            manifest_conn_id="aws_s3_conn",
            # `manifest_conn_id` is optional. If not provided, the default connection ID `aws_default` is used.
            project_name="jaffle_shop",
        ),
        profile_config=profile_config,
        render_config=render_config,
        execution_config=execution_config,
        operator_args={"install_deps": True},
    )
    # [END aws_s3_example]

    # [START gcp_gs_example]
    gcp_gs_example = DbtTaskGroup(
        group_id="gcp_gs_example",
        project_config=ProjectConfig(
            manifest_path="gs://cosmos_remote_target/manifest.json",
            manifest_conn_id="gcp_gs_conn",
            # `manifest_conn_id` is optional. If not provided, the default connection ID `google_cloud_default` is used.
            project_name="jaffle_shop",
        ),
        profile_config=profile_config,
        render_config=render_config,
        execution_config=execution_config,
        operator_args={"install_deps": True},
    )
    # [END gcp_gs_example]

    # [START azure_abfs_example]
    azure_abfs_example = DbtTaskGroup(
        group_id="azure_abfs_example",
        project_config=ProjectConfig(
            manifest_path="abfs://cosmos-manifest-test/manifest.json",
            manifest_conn_id="azure_abfs_conn",
            # `manifest_conn_id` is optional. If not provided, the default connection ID `wasb_default` is used.
            project_name="jaffle_shop",
        ),
        profile_config=profile_config,
        render_config=render_config,
        execution_config=execution_config,
        operator_args={"install_deps": True},
    )
    # [END azure_abfs_example]

    post_dbt = EmptyOperator(task_id="post_dbt")

    (pre_dbt >> local_example >> aws_s3_example >> gcp_gs_example >> azure_abfs_example >> post_dbt)
