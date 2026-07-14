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
    DbtRunOperationGcpGkeOperator,
    DbtSeedGcpGkeOperator,
    DbtSnapshotGcpGkeOperator,
    DbtSourceGcpGkeOperator,
    DbtTestGcpGkeOperator,
)

GKE_KWARGS = {
    "project_id": "my-gcp-project",
    "location": "us-central1",
    "cluster_name": "my-gke-cluster",
}

base_kwargs = {
    **GKE_KWARGS,
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
        assert command_operator.cmds == ["dbt"]
        assert command_operator.arguments == [
            command_name,
            "--vars",
            "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
            "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
            "--no-version-check",
            "--project-dir",
            "my/dir",
        ]


@patch("cosmos.operators._k8s_common.build_kube_args")
@patch("cosmos.operators._k8s_common.build_and_run_cmd")
def test_dbt_gcp_gke_operator_execute(mock_build_and_run, mock_build_kube_args):
    """Tests that the execute method calls build_and_run_cmd."""
    operator = DbtLSGcpGkeOperator(
        project_id="my-gcp-project",
        location="us-central1",
        cluster_name="my-gke-cluster",
        task_id="my-task",
        image="my_image",
        project_dir="my/dir",
    )
    operator.execute(context={})
    mock_build_and_run.assert_called_once()


def test_all_operator_subclasses_instantiate():
    """All GCP GKE operator subclasses can be instantiated without errors."""
    operator_classes = [
        (DbtBuildGcpGkeOperator, {}),
        (DbtLSGcpGkeOperator, {}),
        (DbtRunGcpGkeOperator, {}),
        (DbtSeedGcpGkeOperator, {}),
        (DbtSnapshotGcpGkeOperator, {}),
        (DbtTestGcpGkeOperator, {}),
        (DbtSourceGcpGkeOperator, {}),
        (DbtCloneGcpGkeOperator, {}),
        (DbtRunOperationGcpGkeOperator, {"macro_name": "my_macro"}),
    ]
    for cls, extra_kwargs in operator_classes:
        op = cls(**base_kwargs, **extra_kwargs)
        assert op.project_dir == "my/dir"
