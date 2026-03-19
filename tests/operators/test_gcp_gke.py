from unittest.mock import MagicMock, patch

import pytest

try:
    from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator  # noqa: F401
except ImportError:
    pytest.skip("Google Cloud provider not installed", allow_module_level=True)

from cosmos.operators.gcp_gke import (
    DbtBuildGcpGkeOperator,
    DbtCloneGcpGkeOperator,
    DbtLSGcpGkeOperator,
    DbtRunGcpGkeOperator,
    DbtSeedGcpGkeOperator,
    DbtTestGcpGkeOperator,
)


base_kwargs = {
    "project_id": "my-gcp-project",
    "location": "us-central1",
    "cluster_name": "my-gke-cluster",
    "task_id": "my-task",
    "image": "my_image",
    "project_dir": "my/dir",
    "vars": {
        "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
        "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
    },
    "no_version_check": True,
}


def test_dbt_gcp_gke_build_command():
    """
    Since we know that the GKEStartPodOperator is tested, we can just test that the
    command is built correctly and added to the "arguments" parameter.
    """
    result_map = {
        "ls": DbtLSGcpGkeOperator(**base_kwargs),
        "run": DbtRunGcpGkeOperator(**base_kwargs),
        "test": DbtTestGcpGkeOperator(**base_kwargs),
        "build": DbtBuildGcpGkeOperator(**base_kwargs),
        "seed": DbtSeedGcpGkeOperator(**base_kwargs),
        "clone": DbtCloneGcpGkeOperator(**base_kwargs),
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


@patch("cosmos.operators.gcp_gke.DbtGcpGkeBaseOperator.build_kube_args")
@patch("cosmos.operators.gcp_gke.GKEStartPodOperator.execute")
def test_dbt_gcp_gke_operator_execute(mock_gke_execute, mock_build_kube_args):
    """Tests that the execute method call results in both the build_kube_args method and the GKE execute method being called."""
    operator = DbtLSGcpGkeOperator(
        project_id="my-gcp-project",
        location="us-central1",
        cluster_name="my-gke-cluster",
        task_id="my-task",
        image="my_image",
        project_dir="my/dir",
    )
    operator.execute(context={})
    # Assert that the build_kube_args method was called in the execution
    mock_build_kube_args.assert_called_once()

    # Assert that the GKE execute method was called in the execution
    mock_gke_execute.assert_called_once()
