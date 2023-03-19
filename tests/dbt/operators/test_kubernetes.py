from pathlib import Path
from unittest.mock import MagicMock, patch

from airflow.utils.context import Context
from pendulum import datetime

from cosmos.providers.dbt.core.operators.kubernetes import (
    DbtDepsKubernetesOperator,
    DbtKubernetesBaseOperator,
    DbtLSKubernetesOperator,
    DbtRunKubernetesOperator,
    DbtSeedKubernetesOperator,
    DbtTestKubernetesOperator,
)


def test_dbt_kubernetes_operator_add_global_flags() -> None:
    dbt_kube_operator = DbtKubernetesBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        image="my_image",
        project_dir="my/dir",
        vars={
            "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
            "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
        },
        no_version_check=True,
    )
    assert dbt_kube_operator.add_global_flags() == [
        "--project-dir",
        "my/dir",
        "--vars",
        "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
        "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
        "--no-version-check",
    ]


@patch("cosmos.providers.dbt.core.operators.base.context_to_airflow_vars")
def test_dbt_kubernetes_operator_get_env(p_context_to_airflow_vars: MagicMock) -> None:
    """
    If an end user passes in a
    """
    dbt_kube_operator = DbtKubernetesBaseOperator(
        conn_id="my_airflow_connection",
        task_id="my-task",
        image="my_image",
        project_dir="my/dir",
    )
    dbt_kube_operator.env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "retries": 3,
        ("tuple", "key"): "some_value",
    }
    p_context_to_airflow_vars.return_value = {"START_DATE": "2023-02-15 12:30:00"}
    env = dbt_kube_operator.get_env(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
        profile_vars={
            "SNOWFLAKE_USER": "my_user_id",
            "SNOWFLAKE_PASSWORD": "supersecure123",
            "SNOWFLAKE_ACCOUNT": "my_account",
            "SNOWFLAKE_ROLE": None,
            "SNOWFLAKE_DATABASE": "my_database",
            "SNOWFLAKE_WAREHOUSE": None,
            "SNOWFLAKE_SCHEMA": "jaffle_shop",
        },
    )
    expected_env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "START_DATE": "2023-02-15 12:30:00",
        "SNOWFLAKE_USER": "my_user_id",
        "SNOWFLAKE_PASSWORD": "supersecure123",
        "SNOWFLAKE_ACCOUNT": "my_account",
        "SNOWFLAKE_DATABASE": "my_database",
        "SNOWFLAKE_SCHEMA": "jaffle_shop",
    }
    assert env == expected_env


base_kwargs = {
    "conn_id": "my_airflow_connection",
    "task_id": "my-task",
    "image": "my_image",
    "project_dir": "my/dir",
    "vars": {
        "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
        "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
    },
    "no_version_check": True,
}

result_map = {
    "ls": DbtLSKubernetesOperator(**base_kwargs),
    "run": DbtRunKubernetesOperator(**base_kwargs),
    "test": DbtTestKubernetesOperator(**base_kwargs),
    "deps": DbtDepsKubernetesOperator(**base_kwargs),
    "seed": DbtSeedKubernetesOperator(**base_kwargs),
}


def test_dbt_kubernetes_build_command():
    """
    Since we know that the KubernetesOperator is tested, we can just test that the
    command is built correctly and added to the "arguments" parameter.
    """
    for command_name, command_operator in result_map.items():
        command_operator.build_kube_args(context=MagicMock(), cmd_flags=MagicMock())
        assert command_operator.arguments == [
            "dbt",
            command_name,
            "--project-dir",
            "my/dir",
            "--vars",
            "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
            "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
            "--no-version-check",
        ]
