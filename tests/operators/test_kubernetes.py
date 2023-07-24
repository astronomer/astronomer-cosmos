from unittest.mock import patch

from airflow.utils.context import Context
from pendulum import datetime

from cosmos.config import CosmosConfig, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.operators.kubernetes import (
    DbtLSKubernetesOperator,
)


@patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.hook")
def test_created_pod(test_hook) -> None:
    with patch("pathlib.Path.exists", return_value=True):
        cosmos_config = CosmosConfig(
            project_config=ProjectConfig(
                dbt_project="my/dir",
            ),
            profile_config=ProfileConfig(
                profile_name="default",
                target_name="dev",
                path_to_profiles_yml="my/profiles.yml",
            ),
            execution_config=ExecutionConfig(
                execution_mode="kubernetes",
                dbt_executable_path="dbt",
                dbt_cli_flags=["--no-version-check"],
            ),
        )

    test_hook.is_in_cluster = False
    test_hook._get_namespace.return_value.to_dict.return_value = "foo"

    ls_operator = DbtLSKubernetesOperator(
        task_id="my-task",
        cosmos_config=cosmos_config,
        env={
            "FOO": "BAR",
        },
        image="my_image",
    )
    ls_operator.prepare(context=Context(execution_date=datetime(2023, 2, 15, 12, 30)))

    assert ls_operator.arguments == [
        "dbt",
        "ls",
        "--no-version-check",
    ]

    pod_obj = ls_operator.build_pod_request_obj()
    expected_result = {
        "api_version": "v1",
        "kind": "Pod",
        "metadata": {
            "annotations": {},
            "cluster_name": None,
            "creation_timestamp": None,
            "deletion_grace_period_seconds": None,
            "deletion_timestamp": None,
            "finalizers": None,
            "generate_name": None,
            "generation": None,
            "labels": {
                "airflow_kpo_in_cluster": "False",
                "airflow_version": pod_obj.metadata.labels["airflow_version"],
            },
            "managed_fields": None,
            "name": pod_obj.metadata.name,
            "owner_references": None,
            "resource_version": None,
            "self_link": None,
            "uid": None,
        },
        "spec": {
            "active_deadline_seconds": None,
            "affinity": {},
            "automount_service_account_token": None,
            "containers": [
                {
                    "args": [
                        "dbt",
                        "ls",
                        "--no-version-check",
                    ],
                    "command": [],
                    "env": [{"name": "FOO", "value": "BAR", "value_from": None}],
                    "env_from": [],
                    "image": "my_image",
                    "image_pull_policy": None,
                    "lifecycle": None,
                    "liveness_probe": None,
                    "name": "base",
                    "ports": [],
                    "readiness_probe": None,
                    "resources": None,
                    "security_context": None,
                    "startup_probe": None,
                    "stdin": None,
                    "stdin_once": None,
                    "termination_message_path": None,
                    "termination_message_policy": None,
                    "tty": None,
                    "volume_devices": None,
                    "volume_mounts": [],
                    "working_dir": None,
                }
            ],
            "dns_config": None,
            "dns_policy": None,
            "enable_service_links": None,
            "ephemeral_containers": None,
            "host_aliases": None,
            "host_ipc": None,
            "host_network": False,
            "host_pid": None,
            "hostname": None,
            "image_pull_secrets": [],
            "init_containers": [],
            "node_name": None,
            "node_selector": {},
            "os": None,
            "overhead": None,
            "preemption_policy": None,
            "priority": None,
            "priority_class_name": None,
            "readiness_gates": None,
            "restart_policy": "Never",
            "runtime_class_name": None,
            "scheduler_name": None,
            "security_context": {},
            "service_account": None,
            "service_account_name": None,
            "set_hostname_as_fqdn": None,
            "share_process_namespace": None,
            "subdomain": None,
            "termination_grace_period_seconds": None,
            "tolerations": [],
            "topology_spread_constraints": None,
            "volumes": [],
        },
        "status": None,
    }
    computed_result = pod_obj.to_dict()
    computed_result["metadata"].pop("namespace")

    assert computed_result == expected_result
