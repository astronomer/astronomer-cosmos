from pathlib import Path
from unittest.mock import MagicMock, patch

from airflow.utils.context import Context
from pendulum import datetime

from cosmos.operators.azure_container_instance import (
    DbtAzureContainerInstanceBaseOperator,
    DbtLSAzureContainerInstanceOperator,
    DbtRunAzureContainerInstanceOperator,
    DbtTestAzureContainerInstanceOperator,
    DbtSeedAzureContainerInstanceOperator,
)


class ConcreteDbtAzureContainerInstanceOperator(DbtAzureContainerInstanceBaseOperator):
    base_cmd = ["cmd"]


def test_dbt_azure_container_instance_operator_add_global_flags() -> None:
    dbt_base_operator = ConcreteDbtAzureContainerInstanceOperator(
        ci_conn_id="my_airflow_connection",
        task_id="my-task",
        image="my_image",
        region="Mordor",
        name="my-aci",
        resource_group="my-rg",
        project_dir="my/dir",
        vars={
            "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
            "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
        },
        no_version_check=True,
    )
    assert dbt_base_operator.add_global_flags() == [
        "--vars",
        "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
        "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
        "--no-version-check",
    ]


@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_azure_container_instance_operator_get_env(p_context_to_airflow_vars: MagicMock) -> None:
    """
    If an end user passes in a variable via the context that is also a global flag, validate that the both are kept
    """
    dbt_base_operator = ConcreteDbtAzureContainerInstanceOperator(
        ci_conn_id="my_airflow_connection",
        task_id="my-task",
        image="my_image",
        region="Mordor",
        name="my-aci",
        resource_group="my-rg",
        project_dir="my/dir",
    )
    dbt_base_operator.env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "retries": 3,
        ("tuple", "key"): "some_value",
    }
    p_context_to_airflow_vars.return_value = {"START_DATE": "2023-02-15 12:30:00"}
    env = dbt_base_operator.get_env(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    expected_env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "START_DATE": "2023-02-15 12:30:00",
    }
    assert env == expected_env


@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_azure_container_instance_operator_check_environment_variables(
    p_context_to_airflow_vars: MagicMock,
) -> None:
    """
    If an end user passes in a variable via the context that is also a global flag, validate that the both are kept
    """
    dbt_base_operator = ConcreteDbtAzureContainerInstanceOperator(
        ci_conn_id="my_airflow_connection",
        task_id="my-task",
        image="my_image",
        region="Mordor",
        name="my-aci",
        resource_group="my-rg",
        project_dir="my/dir",
        environment_variables={"FOO": "BAR"},
    )
    dbt_base_operator.env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "retries": 3,
        "FOO": "foo",
        ("tuple", "key"): "some_value",
    }
    expected_env = {"start_date": "20220101", "end_date": "20220102", "some_path": Path(__file__), "FOO": "BAR"}
    dbt_base_operator.build_command(context=MagicMock())

    assert dbt_base_operator.environment_variables == expected_env


base_kwargs = {
    "ci_conn_id": "my_airflow_connection",
    "name": "my-aci",
    "region": "Mordor",
    "resource_group": "my-rg",
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
    "ls": DbtLSAzureContainerInstanceOperator(**base_kwargs),
    "run": DbtRunAzureContainerInstanceOperator(**base_kwargs),
    "test": DbtTestAzureContainerInstanceOperator(**base_kwargs),
    "seed": DbtSeedAzureContainerInstanceOperator(**base_kwargs),
}


def test_dbt_azure_container_instance_build_command():
    """
    Since we know that the AzureContainerInstanceOperator is tested, we can just test that the
    command is built correctly.
    """
    for command_name, command_operator in result_map.items():
        command_operator.build_command(context=MagicMock(), cmd_flags=MagicMock())
        assert command_operator.command == [
            "dbt",
            command_name,
            "--vars",
            "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
            "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
            "--no-version-check",
        ]
