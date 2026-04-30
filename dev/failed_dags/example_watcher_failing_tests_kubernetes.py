"""
DAG to test watcher retry behaviour with ExecutionMode.WATCHER_KUBERNETES.

Uses the watcher_failing_tests dbt project (model_a succeeds, model_f fails).
Producer retries with retries=2 to exercise the XCom backup/restore mechanism.
"""

import os
from datetime import timedelta
from pathlib import Path

from airflow.providers.cncf.kubernetes.secret import Secret
from pendulum import datetime

from cosmos import DbtDag
from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior

DEFAULT_DBT_ROOT_PATH = Path(__file__).resolve().parent.parent / "dags/dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
AIRFLOW_DBT_PROJECT_DIR = DBT_ROOT_PATH / "watcher_failing_tests"

K8S_PROJECT_DIR = "dags/dbt/watcher_failing_tests"
K8S_DBT_PROFILES_YAML_FILEPATH = Path(K8S_PROJECT_DIR) / "profiles.yml"

DBT_IMAGE = "dbt-watcher-failing-tests:1.0.0"

postgres_password_secret = Secret(
    deploy_type="env",
    deploy_target="POSTGRES_PASSWORD",
    secret="postgres-secrets",
    key="password",
)

postgres_host_secret = Secret(
    deploy_type="env",
    deploy_target="POSTGRES_HOST",
    secret="postgres-secrets",
    key="host",
)

operator_args = {
    "deferrable": False,
    "image": DBT_IMAGE,
    "get_logs": True,
    "is_delete_operator_pod": False,
    "log_events_on_failure": True,
    "secrets": [postgres_password_secret, postgres_host_secret],
    "env_vars": {
        "POSTGRES_DB": "postgres",
        "POSTGRES_SCHEMA": "public",
        "POSTGRES_USER": "postgres",
    },
    "execution_timeout": timedelta(seconds=120),
}

if os.getenv("CI"):
    operator_args["trigger_rule"] = "all_success"

profile_config = ProfileConfig(
    profile_name="postgres_profile",
    target_name="dev",
    profiles_yml_filepath=K8S_DBT_PROFILES_YAML_FILEPATH,
)

project_config = ProjectConfig(
    project_name="watcher_failing_tests",
    manifest_path=AIRFLOW_DBT_PROJECT_DIR / "target/manifest.json",
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_MANIFEST,
    test_behavior=TestBehavior.NONE,
)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=0),
}

example_watcher_failing_tests_kubernetes = DbtDag(
    dag_id="example_watcher_failing_tests_kubernetes",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    project_config=project_config,
    profile_config=profile_config,
    render_config=render_config,
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER_KUBERNETES,
        dbt_project_path=K8S_PROJECT_DIR,
    ),
    operator_args=operator_args,
    default_args=default_args,
)
