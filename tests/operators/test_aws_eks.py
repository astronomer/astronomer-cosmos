from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from cosmos.operators.aws_eks import (
    DbtBuildEksOperator,
    DbtLSEksOperator,
    DbtRunEksOperator,
    DbtSeedEksOperator,
    DbtTestEksOperator,
)


@pytest.fixture()
def mock_kubernetes_execute():
    with patch("cosmos.operators.kubernetes.KubernetesPodOperator.execute") as mock_execute:
        yield mock_execute


base_kwargs = {
    "conn_id": "my_airflow_connection",
    "cluster_name": "my-cluster",
    "task_id": "my-task",
    "image": "my_image",
    "project_dir": "my/dir",
    "vars": {
        "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
        "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
    },
    "no_version_check": True,
}


def test_dbt_kubernetes_build_command():
    """
    Since we know that the KubernetesOperator is tested, we can just test that the
    command is built correctly and added to the "arguments" parameter.
    """

    result_map = {
        "ls": DbtLSEksOperator(**base_kwargs),
        "run": DbtRunEksOperator(**base_kwargs),
        "test": DbtTestEksOperator(**base_kwargs),
        "build": DbtBuildEksOperator(**base_kwargs),
        "seed": DbtSeedEksOperator(**base_kwargs),
    }

    for command_name, command_operator in result_map.items():
        command_operator.build_kube_args(context=MagicMock(), cmd_flags=MagicMock())
        assert command_operator.arguments == [
            "dbt",
            command_name,
            "--vars",
            "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
            "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
            "--no-version-check",
            "--project-dir",
            "my/dir",
        ]


@patch("cosmos.operators.kubernetes.DbtKubernetesBaseOperator.build_kube_args")
@patch("cosmos.operators.aws_eks.EksHook.generate_config_file")
def test_dbt_kubernetes_operator_execute(mock_generate_config_file, mock_build_kube_args, mock_kubernetes_execute):
    """Tests that the execute method call results in both the build_kube_args method and the kubernetes execute method being called."""
    operator = DbtLSEksOperator(
        conn_id="my_airflow_connection",
        cluster_name="my-cluster",
        task_id="my-task",
        image="my_image",
        project_dir="my/dir",
    )
    operator.execute(context={})
    # Assert that the build_kube_args method was called in the execution
    mock_build_kube_args.assert_called_once()

    # Assert that the generate_config_file method was called in the execution to create the kubeconfig for eks
    mock_generate_config_file.assert_called_once_with(eks_cluster_name="my-cluster", pod_namespace="default")

    # Assert that the kubernetes execute method was called in the execution
    mock_kubernetes_execute.assert_called_once()
    assert mock_kubernetes_execute.call_args.args[-1] == {}


def test_provided_config_file_fails():
    """Tests that the constructor fails if it is called with a config_file."""
    with pytest.raises(AirflowException) as err_context:
        DbtLSEksOperator(
            conn_id="my_airflow_connection",
            cluster_name="my-cluster",
            task_id="my-task",
            image="my_image",
            project_dir="my/dir",
            config_file="my/config",
        )
    assert "The config_file is not an allowed parameter for the EksPodOperator." in str(err_context.value)
