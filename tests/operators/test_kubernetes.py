from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pendulum import datetime

from cosmos.operators.kubernetes import (
    DbtBuildKubernetesOperator,
    DbtKubernetesBaseOperator,
    DbtLSKubernetesOperator,
    DbtRunKubernetesOperator,
    DbtSeedKubernetesOperator,
    DbtTestKubernetesOperator,
)

from airflow.utils.context import Context, context_merge
from airflow.models import TaskInstance

try:
    from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction

    module_available = True
except ImportError:
    module_available = False


class ConcreteDbtKubernetesBaseOperator(DbtKubernetesBaseOperator):
    base_cmd = ["cmd"]


def test_dbt_kubernetes_operator_add_global_flags() -> None:
    dbt_kube_operator = ConcreteDbtKubernetesBaseOperator(
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


@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_kubernetes_operator_get_env(p_context_to_airflow_vars: MagicMock) -> None:
    """
    If an end user passes in a
    """
    dbt_kube_operator = ConcreteDbtKubernetesBaseOperator(
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
    "build": DbtBuildKubernetesOperator(**base_kwargs),
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
            "--project-dir",
            "my/dir",
        ]


@pytest.mark.parametrize(
    "additional_kwargs,expected_results",
    [
        ({"on_success_callback": None, "is_delete_operator_pod": True}, (1, 1, True, "delete_pod")),
        (
            {"on_success_callback": (lambda **kwargs: None), "is_delete_operator_pod": False},
            (2, 1, False, "keep_pod"),
        ),
        (
            {"on_success_callback": [(lambda **kwargs: None), (lambda **kwargs: None)], "is_delete_operator_pod": None},
            (3, 1, True, "delete_pod"),
        ),
        (
            {"on_failure_callback": None, "is_delete_operator_pod": True, "on_finish_action": "keep_pod"},
            (1, 1, True, "delete_pod"),
        ),
        (
            {
                "on_failure_callback": (lambda **kwargs: None),
                "is_delete_operator_pod": None,
                "on_finish_action": "delete_pod",
            },
            (1, 2, True, "delete_pod"),
        ),
        (
            {
                "on_failure_callback": [(lambda **kwargs: None), (lambda **kwargs: None)],
                "is_delete_operator_pod": None,
                "on_finish_action": "delete_succeeded_pod",
            },
            (1, 3, False, "delete_succeeded_pod"),
        ),
        ({"is_delete_operator_pod": None, "on_finish_action": "keep_pod"}, (1, 1, False, "keep_pod")),
        ({}, (1, 1, True, "delete_pod")),
    ],
)
@pytest.mark.skipif(
    not module_available, reason="Kubernetes module `airflow.providers.cncf.kubernetes.utils.pod_manager` not available"
)
def test_dbt_test_kubernetes_operator_constructor(additional_kwargs, expected_results):
    test_operator = DbtTestKubernetesOperator(
        on_warning_callback=(lambda **kwargs: None), **additional_kwargs, **base_kwargs
    )

    print(additional_kwargs, test_operator.__dict__)

    assert isinstance(test_operator.on_success_callback, list)
    assert isinstance(test_operator.on_failure_callback, list)
    assert test_operator._handle_warnings in test_operator.on_success_callback
    assert test_operator._cleanup_pod in test_operator.on_failure_callback
    assert len(test_operator.on_success_callback) == expected_results[0]
    assert len(test_operator.on_failure_callback) == expected_results[1]
    assert test_operator.is_delete_operator_pod_original == expected_results[2]
    assert test_operator.on_finish_action_original == OnFinishAction(expected_results[3])


class FakePodManager:
    def read_pod_logs(self, pod, container):
        assert pod == "pod"
        assert container == "base"
        log_string = """
19:48:25  Concurrency: 4 threads (target='target')
19:48:25
19:48:25  1 of 2 START test dbt_utils_accepted_range_table_col__12__0 ................... [RUN]
19:48:25  2 of 2 START test unique_table__uuid .......................................... [RUN]
19:48:27  1 of 2 WARN 252 dbt_utils_accepted_range_table_col__12__0 ..................... [WARN 117 in 1.83s]
19:48:27  2 of 2 PASS unique_table__uuid ................................................ [PASS in 1.85s]
19:48:27
19:48:27  Finished running 2 tests, 1 hook in 0 hours 0 minutes and 12.86 seconds (12.86s).
19:48:27
19:48:27  Completed with 1 warning:
19:48:27
19:48:27  Warning in test dbt_utils_accepted_range_table_col__12__0 (models/ads/ads.yaml)
19:48:27  Got 252 results, configured to warn if >0
19:48:27
19:48:27    compiled Code at target/compiled/model/models/table/table.yaml/dbt_utils_accepted_range_table_col__12__0.sql
19:48:27
19:48:27  Done. PASS=1 WARN=1 ERROR=0 SKIP=0 TOTAL=2
"""
        return (log.encode("utf-8") for log in log_string.split("\n"))


@pytest.mark.skipif(
    not module_available, reason="Kubernetes module `airflow.providers.cncf.kubernetes.utils.pod_manager` not available"
)
def test_dbt_test_kubernetes_operator_handle_warnings_and_cleanup_pod():
    def on_warning_callback(context: Context):
        assert context["test_names"] == ["dbt_utils_accepted_range_table_col__12__0"]
        assert context["test_results"] == ["Got 252 results, configured to warn if >0"]

    def cleanup(pod: str, remote_pod: str):
        assert pod == remote_pod

    test_operator = DbtTestKubernetesOperator(
        is_delete_operator_pod=True, on_warning_callback=on_warning_callback, **base_kwargs
    )
    task_instance = TaskInstance(test_operator)
    task_instance.task.pod_manager = FakePodManager()
    task_instance.task.pod = task_instance.task.remote_pod = "pod"
    task_instance.task.cleanup = cleanup

    context = Context()
    context_merge(context, task_instance=task_instance)

    test_operator._handle_warnings(context)


@patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.hook")
@patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook")
def test_created_pod(test_hook1, test_hook2):
    test_hook1.is_in_cluster = False
    test_hook2.is_in_cluster = False
    test_hook1._get_namespace.return_value.to_dict.return_value = "foo"
    test_hook2._get_namespace.return_value.to_dict.return_value = "foo"
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
                        "--project-dir",
                        "my/dir",
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
                    # "termination_message_policy": None,
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
    computed_result["spec"]["containers"][0].pop("termination_message_policy")
    computed_result["metadata"].pop("namespace")
    assert computed_result == expected_result
