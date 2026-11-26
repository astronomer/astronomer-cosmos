"""Pin the dbt profile flags that each execution mode puts on the dbt command.

``ProfileConfig`` is turned into the ``--profile``, ``--target`` and ``--profiles-dir`` dbt flags, and every execution
mode builds its dbt command its own way. These tests pin the exact command each mode builds for each kind of
``ProfileConfig``, including one that arrives through the DAG ``default_args``, so that changes to the shared profile
handling in ``AbstractDbtBase`` cannot silently change the command a mode runs.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from airflow.models import DAG
from pendulum import datetime

from cosmos import ProfileConfig
from cosmos.constants import InvocationMode

PROFILES_YML_CONFIG = ProfileConfig(
    profile_name="my_profile",
    target_name="my_target",
    profiles_yml_filepath="/path/to/profiles.yml",
)
PROFILE_MAPPING_CONFIG = ProfileConfig(
    profile_name="my_profile",
    target_name="my_target",
    profile_mapping=MagicMock(),
)
EMPTY_NAMES_CONFIG = ProfileConfig(
    profile_name="",
    target_name="",
    profiles_yml_filepath="/path/to/profiles.yml",
)
DEFAULT_ARGS_CONFIG = ProfileConfig(
    profile_name="default_args_profile",
    target_name="default_args_target",
    profiles_yml_filepath="/path/to/default_args/profiles.yml",
)

USER_FLAGS = ["--threads", "2"]
PROFILE_FLAGS = ["--profile", "my_profile", "--target", "my_target"]
PROJECT_DIR_FLAGS = ["--project-dir", "my/dir"]


def _common_kwargs() -> dict[str, Any]:
    # A fresh list each time: some operators extend ``dbt_cmd_flags`` in place (the watcher producer adds
    # ``--log-format json``), which would otherwise leak into the next test.
    return {"project_dir": "my/dir", "dbt_cmd_flags": list(USER_FLAGS)}


def _build_kubernetes(**kwargs: Any) -> tuple[Any, list[str]]:
    from cosmos.operators.kubernetes import DbtRunKubernetesOperator

    operator = DbtRunKubernetesOperator(image="my_image", **kwargs)
    operator.build_kube_args(context=MagicMock(), cmd_flags=operator.add_cmd_flags())
    return operator, operator.arguments


def _build_gcp_gke(**kwargs: Any) -> tuple[Any, list[str]]:
    pytest.importorskip("airflow.providers.google.cloud.operators.kubernetes_engine")
    from cosmos.operators.gcp_gke import DbtRunGcpGkeOperator

    operator = DbtRunGcpGkeOperator(
        image="my_image",
        project_id="my-gcp-project",
        location="us-central1",
        cluster_name="my-gke-cluster",
        **kwargs,
    )
    operator.build_kube_args(context=MagicMock(), cmd_flags=operator.add_cmd_flags())
    return operator, operator.arguments


def _build_aws_eks(**kwargs: Any) -> tuple[Any, list[str]]:
    pytest.importorskip("airflow.providers.amazon.aws.hooks.eks")
    from cosmos.operators.aws_eks import DbtRunAwsEksOperator

    operator = DbtRunAwsEksOperator(image="my_image", cluster_name="my-cluster", **kwargs)
    operator.build_kube_args(context=MagicMock(), cmd_flags=operator.add_cmd_flags())
    return operator, operator.arguments


def _build_watcher_kubernetes_producer(**kwargs: Any) -> tuple[Any, list[str]]:
    from cosmos.operators.watcher_kubernetes import DbtProducerWatcherKubernetesOperator

    operator = DbtProducerWatcherKubernetesOperator(image="my_image", **kwargs)
    operator.build_kube_args(context=MagicMock(), cmd_flags=operator.add_cmd_flags())
    return operator, operator.arguments


def _build_watcher_gcp_gke_producer(**kwargs: Any) -> tuple[Any, list[str]]:
    pytest.importorskip("airflow.providers.google.cloud.operators.kubernetes_engine")
    from cosmos.operators.watcher_gcp_gke import DbtProducerWatcherGcpGkeOperator

    operator = DbtProducerWatcherGcpGkeOperator(
        image="my_image",
        project_id="my-gcp-project",
        location="us-central1",
        cluster_name="my-gke-cluster",
        **kwargs,
    )
    operator.build_kube_args(context=MagicMock(), cmd_flags=operator.add_cmd_flags())
    return operator, operator.arguments


def _build_gcp_cloud_run_job(**kwargs: Any) -> tuple[Any, list[str]]:
    pytest.importorskip("airflow.providers.google.cloud.operators.cloud_run")
    from cosmos.operators.gcp_cloud_run_job import DbtRunGcpCloudRunJobOperator

    operator = DbtRunGcpCloudRunJobOperator(
        project_id="my-gcp-project",
        region="europe-west1",
        job_name="my-dbt-job",
        **kwargs,
    )
    operator.build_command(context=MagicMock(), cmd_flags=operator.add_cmd_flags())
    return operator, operator.command


def _build_docker(**kwargs: Any) -> tuple[Any, list[str]]:
    pytest.importorskip("airflow.providers.docker.operators.docker")
    from cosmos.operators.docker import DbtRunDockerOperator

    operator = DbtRunDockerOperator(image="my_image", **kwargs)
    operator.build_command(context=MagicMock(), cmd_flags=operator.add_cmd_flags())
    return operator, operator.command


def _build_aws_ecs(**kwargs: Any) -> tuple[Any, list[str]]:
    pytest.importorskip("airflow.providers.amazon.aws.operators.ecs")
    from cosmos.operators.aws_ecs import DbtRunAwsEcsOperator

    operator = DbtRunAwsEcsOperator(cluster="my-ecs-cluster", task_definition="my-dbt-task-definition", **kwargs)
    operator.build_command(context=MagicMock(), cmd_flags=operator.add_cmd_flags())
    return operator, operator.command


def _build_azure_container_instance(**kwargs: Any) -> tuple[Any, list[str]]:
    pytest.importorskip("airflow.providers.microsoft.azure.operators.container_instances")
    from cosmos.operators.azure_container_instance import DbtRunAzureContainerInstanceOperator

    operator = DbtRunAzureContainerInstanceOperator(
        ci_conn_id="my_airflow_connection",
        resource_group="my-rg",
        name="my-aci",
        image="my_image",
        region="Mordor",
        **kwargs,
    )
    operator.build_command(context=MagicMock(), cmd_flags=operator.add_cmd_flags())
    return operator, operator.command


Builder = Callable[..., tuple[Any, list[str]]]

# Modes that add --profile/--target (when set) and --project-dir after the user-supplied dbt flags.
MODES_WITH_PROFILE_FLAGS: dict[str, Builder] = {
    "kubernetes": _build_kubernetes,
    "gcp_gke": _build_gcp_gke,
    "aws_eks": _build_aws_eks,
    "watcher_kubernetes_producer": _build_watcher_kubernetes_producer,
    "watcher_gcp_gke_producer": _build_watcher_gcp_gke_producer,
    "gcp_cloud_run_job": _build_gcp_cloud_run_job,
}

# Modes that accept a ProfileConfig but rely on the profiles.yml and project inside the container.
MODES_WITHOUT_PROFILE_FLAGS: dict[str, Builder] = {
    "docker": _build_docker,
    "aws_ecs": _build_aws_ecs,
    "azure_container_instance": _build_azure_container_instance,
}

ALL_CONTAINER_MODES: dict[str, Builder] = {**MODES_WITH_PROFILE_FLAGS, **MODES_WITHOUT_PROFILE_FLAGS}

# Modes whose operators reject a ProfileConfig that uses a profile_mapping.
MODES_REQUIRING_PROFILES_YML = {"docker"}


def _base_command(mode: str) -> list[str]:
    if mode in ("watcher_kubernetes_producer", "watcher_gcp_gke_producer"):
        # The producer runs ``dbt build`` and asks dbt for JSON logs, which it parses for per-node status.
        return ["dbt", "build", *USER_FLAGS, "--log-format", "json"]
    return ["dbt", "run", *USER_FLAGS]


# The flags each kind of ProfileConfig adds after the user-supplied dbt flags, for modes in MODES_WITH_PROFILE_FLAGS.
# Modes in MODES_WITHOUT_PROFILE_FLAGS add nothing for any of them.
PROFILE_CONFIG_CASES: dict[str, tuple[ProfileConfig | None, list[str]]] = {
    "profiles_yml": (PROFILES_YML_CONFIG, [*PROFILE_FLAGS, *PROJECT_DIR_FLAGS]),
    "profile_mapping": (PROFILE_MAPPING_CONFIG, [*PROFILE_FLAGS, *PROJECT_DIR_FLAGS]),
    "empty_names": (EMPTY_NAMES_CONFIG, PROJECT_DIR_FLAGS),
    "no_profile_config": (None, PROJECT_DIR_FLAGS),
}
DEFAULT_ARGS_FLAGS = ["--profile", "default_args_profile", "--target", "default_args_target", *PROJECT_DIR_FLAGS]


def _expected_command(mode: str, flags_with_profile_support: list[str]) -> list[str]:
    if mode in MODES_WITHOUT_PROFILE_FLAGS:
        return _base_command(mode)
    return _base_command(mode) + flags_with_profile_support


def _container_cases() -> list[Any]:
    cases = []
    for mode in ALL_CONTAINER_MODES:
        for case_name, (profile_config, expected_flags) in PROFILE_CONFIG_CASES.items():
            if mode in MODES_REQUIRING_PROFILES_YML and case_name == "profile_mapping":
                continue
            cases.append(pytest.param(mode, profile_config, expected_flags, id=f"{mode}-{case_name}"))
    return cases


@pytest.mark.parametrize("mode, profile_config, expected_flags", _container_cases())
def test_container_mode_dbt_command(mode: str, profile_config: ProfileConfig | None, expected_flags: list[str]) -> None:
    operator, command = ALL_CONTAINER_MODES[mode](task_id="my-task", profile_config=profile_config, **_common_kwargs())

    assert operator.profile_config is profile_config
    assert command == _expected_command(mode, expected_flags)


def test_kubernetes_profile_flags_follow_user_flags_exactly() -> None:
    """Spell out one full command so the expected shape is readable without the helpers above."""
    _, command = _build_kubernetes(task_id="my-task", profile_config=PROFILES_YML_CONFIG, **_common_kwargs())

    assert command == [
        "dbt",
        "run",
        "--threads",
        "2",
        "--profile",
        "my_profile",
        "--target",
        "my_target",
        "--project-dir",
        "my/dir",
    ]


@pytest.mark.parametrize("mode", list(ALL_CONTAINER_MODES))
def test_container_mode_profile_config_from_default_args(mode: str) -> None:
    with DAG(
        "test-profile-config-default-args",
        start_date=datetime(2024, 1, 1),
        default_args={"profile_config": DEFAULT_ARGS_CONFIG},
    ):
        operator, command = ALL_CONTAINER_MODES[mode](task_id="my-task", **_common_kwargs())

    assert operator.profile_config == DEFAULT_ARGS_CONFIG
    assert command == _expected_command(mode, DEFAULT_ARGS_FLAGS)


@pytest.mark.parametrize("mode", list(ALL_CONTAINER_MODES))
def test_container_mode_explicit_profile_config_wins_over_default_args(mode: str) -> None:
    with DAG(
        "test-profile-config-precedence",
        start_date=datetime(2024, 1, 1),
        default_args={"profile_config": DEFAULT_ARGS_CONFIG},
    ):
        operator, command = ALL_CONTAINER_MODES[mode](
            task_id="my-task", profile_config=PROFILES_YML_CONFIG, **_common_kwargs()
        )

    assert operator.profile_config is PROFILES_YML_CONFIG
    assert command == _expected_command(mode, [*PROFILE_FLAGS, *PROJECT_DIR_FLAGS])


def _build_local(**kwargs: Any) -> Any:
    from cosmos.operators.local import DbtRunLocalOperator

    return DbtRunLocalOperator(invocation_mode=InvocationMode.SUBPROCESS, install_deps=False, **kwargs)


@pytest.mark.parametrize(
    "profile_config, expected_profile_flags",
    [
        pytest.param(PROFILES_YML_CONFIG, PROFILE_FLAGS, id="profiles_yml"),
        pytest.param(PROFILE_MAPPING_CONFIG, PROFILE_FLAGS, id="profile_mapping"),
        # Local mode renders profiles.yml under these names, so it passes them even when they are empty.
        pytest.param(EMPTY_NAMES_CONFIG, ["--profile", "", "--target", ""], id="empty_names"),
    ],
)
def test_local_mode_dbt_command(profile_config: ProfileConfig, expected_profile_flags: list[str]) -> None:
    operator = _build_local(task_id="my-task", profile_config=profile_config, **_common_kwargs())

    command, _ = operator.build_cmd(context=MagicMock(), cmd_flags=operator.add_cmd_flags())
    runtime_flags = operator._generate_dbt_flags("/tmp/my-project", Path("/tmp/profiles/profiles.yml"))

    assert operator.profile_config is profile_config
    assert command[1:] == ["run", *USER_FLAGS]
    assert runtime_flags == [
        "--project-dir",
        "/tmp/my-project",
        "--profiles-dir",
        "/tmp/profiles",
        *expected_profile_flags,
    ]


def test_local_mode_profile_config_from_default_args() -> None:
    with DAG(
        "test-local-profile-config-default-args",
        start_date=datetime(2024, 1, 1),
        default_args={"profile_config": DEFAULT_ARGS_CONFIG},
    ):
        operator = _build_local(task_id="my-task", **_common_kwargs())

    assert operator.profile_config == DEFAULT_ARGS_CONFIG
    assert operator._generate_dbt_flags("/tmp/my-project", Path("/tmp/profiles/profiles.yml"))[-4:] == [
        "--profile",
        "default_args_profile",
        "--target",
        "default_args_target",
    ]


def test_local_mode_explicit_profile_config_wins_over_default_args() -> None:
    with DAG(
        "test-local-profile-config-precedence",
        start_date=datetime(2024, 1, 1),
        default_args={"profile_config": DEFAULT_ARGS_CONFIG},
    ):
        operator = _build_local(task_id="my-task", profile_config=PROFILES_YML_CONFIG, **_common_kwargs())

    assert operator.profile_config is PROFILES_YML_CONFIG
    assert operator._generate_dbt_flags("/tmp/my-project", Path("/tmp/profiles/profiles.yml"))[-4:] == PROFILE_FLAGS
