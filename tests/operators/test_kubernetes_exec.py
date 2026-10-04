import importlib
import runpy
from pathlib import Path
from unittest.mock import patch

import pytest

pytest.importorskip("airflow.providers.cncf.kubernetes.operators.pod_exec")

from cosmos import DbtDag
from cosmos.airflow.graph import calculate_operator_class
from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode
from cosmos.operators import kubernetes_exec
from cosmos.operators.kubernetes_exec import DbtRunKubernetesExecOperator


@pytest.mark.parametrize(
    "dbt_class,kwargs,command",
    [
        ("DbtRun", {"full_refresh": True}, ["run", "--full-refresh"]),
        ("DbtBuild", {"full_refresh": True}, ["build", "--full-refresh"]),
        ("DbtSeed", {"full_refresh": True}, ["seed", "--full-refresh"]),
        ("DbtSnapshot", {}, ["snapshot"]),
        ("DbtTest", {}, ["test"]),
        ("DbtSource", {}, ["source", "freshness"]),
        ("DbtLS", {}, ["ls"]),
        ("DbtRunOperation", {"macro_name": "my_macro"}, ["run-operation", "my_macro"]),
        ("DbtClone", {"full_refresh": True}, ["clone", "--full-refresh"]),
    ],
)
@patch.object(kubernetes_exec.KubernetesPodExecOperator, "execute", autospec=True)
def test_execute_generated_operator(mock_execute, dbt_class, kwargs, command):
    class_path = calculate_operator_class(ExecutionMode.KUBERNETES_EXEC, dbt_class)
    module_name, class_name = class_path.rsplit(".", 1)
    operator_class = getattr(importlib.import_module(module_name), class_name)
    operator = operator_class(task_id="dbt_task", pod_name="warm-dbt", project_dir="/dbt/project", **kwargs)

    operator.execute({})

    assert list(operator.command) == ["env", "--", "dbt", *command, "--project-dir", "/dbt/project"]
    mock_execute.assert_called_once_with(operator, {})


@patch.object(kubernetes_exec.KubernetesPodExecOperator, "execute", autospec=True)
def test_forward_dbt_options_and_container_connection(mock_execute):
    operator = DbtRunKubernetesExecOperator(
        task_id="dbt_task",
        pod_name="warm-dbt",
        container_name="dbt",
        namespace="analytics",
        kubernetes_conn_id="cluster",
        project_dir="/dbt/project",
        dbt_executable_path="/opt/dbt/bin/dbt",
        select="orders",
        dbt_cmd_flags=["--threads", "2"],
        profile_config=ProfileConfig(
            profile_name="warehouse", target_name="prod", profiles_yml_filepath="/dbt/profiles.yml"
        ),
        env={"VALUE": "spaces; $(not-a-shell)", "BYTES": b"hello"},
    )

    operator.execute({})

    assert list(operator.command) == [
        "env",
        "--",
        "VALUE=spaces; $(not-a-shell)",
        "BYTES=hello",
        "/opt/dbt/bin/dbt",
        "run",
        "--select",
        "orders",
        "--threads",
        "2",
        "--profile",
        "warehouse",
        "--target",
        "prod",
        "--project-dir",
        "/dbt/project",
    ]
    forwarded = mock_execute.call_args.args[0]
    assert (forwarded.pod_name, forwarded.container_name, forwarded.namespace, forwarded.kubernetes_conn_id) == (
        "warm-dbt",
        "dbt",
        "analytics",
        "cluster",
    )


@patch.object(kubernetes_exec.KubernetesPodExecOperator, "execute", autospec=True)
def test_interceptor_runs_before_building_command(mock_execute):
    def interceptor(context, operator):
        operator.select = "selected_by_interceptor"
        operator.env = {"RUN": "intercepted"}

    operator = DbtRunKubernetesExecOperator(
        task_id="dbt_task", pod_name="warm", project_dir="/dbt", interceptors=[interceptor]
    )
    operator.execute({})
    assert list(operator.command) == [
        "env",
        "--",
        "RUN=intercepted",
        "dbt",
        "run",
        "--select",
        "selected_by_interceptor",
        "--project-dir",
        "/dbt",
    ]


@patch.object(
    kubernetes_exec.KubernetesPodExecOperator, "execute", autospec=True, side_effect=RuntimeError("exec failed")
)
def test_exec_failure_propagates(mock_execute):
    operator = DbtRunKubernetesExecOperator(task_id="dbt_task", pod_name="warm", project_dir="/dbt")
    with pytest.raises(RuntimeError, match="exec failed"):
        operator.execute({})


@pytest.mark.parametrize(
    "kwargs,message",
    [({"command": ["dbt"]}, "Cosmos builds"), ({"on_warning_callback": lambda context: None}, "not supported")],
)
def test_reject_unsupported_arguments(kwargs, message):
    with pytest.raises(ValueError, match=message):
        DbtRunKubernetesExecOperator(task_id="dbt_task", pod_name="warm", project_dir="/dbt", **kwargs)


@patch.dict("sys.modules", {"airflow.providers.cncf.kubernetes.operators.pod_exec": None})
def test_missing_provider_has_actionable_error():
    with pytest.raises(ImportError, match="cncf-kubernetes>=10.22.0"):
        runpy.run_path(kubernetes_exec.__file__)


def test_render_pod_and_dbt_parameters():
    operator = DbtRunKubernetesExecOperator(
        task_id="dbt_task",
        pod_name="{{ params.pod }}",
        project_dir="/dbt",
        full_refresh="{{ params.refresh }}",
        select="{{ params.model }}",
    )
    operator.render_template_fields({"params": {"pod": "warm", "refresh": "true", "model": "orders"}})
    assert (operator.pod_name, operator.select, operator.add_cmd_flags()) == ("warm", "orders", ["--full-refresh"])


def test_manifest_generates_tasks_for_existing_pod():
    dag = DbtDag(
        dag_id="existing_pod",
        project_config=ProjectConfig(
            manifest_path=Path(__file__).parents[1] / "sample/manifest.json", project_name="example"
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.KUBERNETES_EXEC, dbt_project_path="/dbt/project", dbt_executable_path="dbt"
        ),
        render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
        operator_args={"pod_name": "warm-dbt", "namespace": "analytics"},
    )
    assert dag.tasks
    for task in dag.tasks:
        assert isinstance(task, kubernetes_exec.DbtKubernetesExecBaseOperator)
        assert task.pod_name == "warm-dbt"
    assert any(task.upstream_task_ids for task in dag.tasks)
