from pathlib import Path
from unittest.mock import MagicMock, patch

from airflow.utils.context import Context
from pendulum import datetime

from cosmos.providers.dbt.core.operators.kubernetes import (
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
    )
    expected_env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "START_DATE": "2023-02-15 12:30:00",
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
            "--vars",
            "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
            "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
            "--no-version-check",
        ]


@patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.hook")
def test_created_pod(test_hook):
    test_hook.is_in_cluster = False
    test_hook._get_namespace.return_value.to_dict.return_value = "foo"
    ls_kwargs = {"env_vars": {"FOO": "BAR"}}
    ls_kwargs.update(base_kwargs)
    ls_operator = DbtLSKubernetesOperator(**ls_kwargs)
    ls_operator.build_kube_args(context={}, cmd_flags=MagicMock())
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
                        "--vars",
                        "end_time: '{{ "
                        "data_interval_end.strftime(''%Y%m%d%H%M%S'') "
                        "}}'\n"
                        "start_time: '{{ "
                        "data_interval_start.strftime(''%Y%m%d%H%M%S'') "
                        "}}'\n",
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
