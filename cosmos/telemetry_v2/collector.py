"""Telemetry collection utilities for Cosmos metrics."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

from cosmos.telemetry_v2 import emit_metric
from cosmos.telemetry_v2.base import MetricType, TelemetryMetric

if TYPE_CHECKING:
    from airflow.models.dag import DAG
    from airflow.utils.task_group import TaskGroup

    from cosmos.operators.base import AbstractDbtBase


def _get_operator_class_name(operator: AbstractDbtBase) -> str:
    """Get the operator class name, handling subclasses."""
    return operator.__class__.__name__


def _is_subclassed_operator(operator: AbstractDbtBase) -> bool:
    """Check if the operator is a user-defined subclass of a Cosmos operator."""
    # Get the module where the operator class is defined
    module = inspect.getmodule(operator.__class__)
    if not module:
        return False

    # If the module is not in cosmos.operators, it's likely a user subclass
    module_name = module.__name__
    cosmos_operator_modules = {
        "cosmos.operators.local",
        "cosmos.operators.docker",
        "cosmos.operators.kubernetes",
        "cosmos.operators.virtualenv",
        "cosmos.operators.aws_ecs",
        "cosmos.operators.aws_eks",
        "cosmos.operators.azure_container_instance",
        "cosmos.operators.gcp_cloud_run_job",
        "cosmos.operators._asynchronous",
    }

    # Check if it's a direct Cosmos module or a submodule
    for cosmos_module in cosmos_operator_modules:
        if module_name.startswith(cosmos_module):
            return False

    return True


def _get_dbt_command(operator: AbstractDbtBase) -> str:
    """Extract the dbt command from the operator's base_cmd."""
    try:
        base_cmd = operator.base_cmd
        if base_cmd and len(base_cmd) > 0:
            # base_cmd is typically like ['run'], ['test'], ['build'], etc.
            return base_cmd[0] if isinstance(base_cmd, list) else str(base_cmd)
    except Exception:
        pass
    return "unknown"


def _get_execution_mode(operator: AbstractDbtBase) -> str:
    """Extract execution mode from operator class name or module."""
    class_name = operator.__class__.__name__

    # Map operator class names to execution modes
    execution_mode_map = {
        "Local": "local",
        "Docker": "docker",
        "Kubernetes": "kubernetes",
        "Virtualenv": "virtualenv",
        "AwsEcs": "aws_ecs",
        "AwsEks": "aws_eks",
        "AzureContainerInstance": "azure_container_instance",
        "GcpCloudRunJob": "gcp_cloud_run_job",
        "AirflowAsync": "airflow_async",
        "Watcher": "watcher",
    }

    for key, mode in execution_mode_map.items():
        if key in class_name:
            return mode

    # Fallback: try to extract from module
    module = inspect.getmodule(operator.__class__)
    if module:
        module_name = module.__name__
        if "local" in module_name:
            return "local"
        elif "docker" in module_name:
            return "docker"
        elif "kubernetes" in module_name:
            return "kubernetes"
        elif "virtualenv" in module_name:
            return "virtualenv"
        elif "aws_ecs" in module_name:
            return "aws_ecs"
        elif "aws_eks" in module_name:
            return "aws_eks"
        elif "azure_container_instance" in module_name:
            return "azure_container_instance"
        elif "gcp_cloud_run_job" in module_name:
            return "gcp_cloud_run_job"
        elif "_asynchronous" in module_name:
            return "airflow_async"
        elif "watcher" in module_name:
            return "watcher"

    return "unknown"


def _get_profile_type(operator: AbstractDbtBase) -> str | None:
    """Extract profile type from operator's profile_config."""
    if hasattr(operator, "profile_config") and operator.profile_config:
        try:
            return operator.profile_config.get_profile_type()
        except Exception:
            pass
    return None


def _get_profile_method(operator: AbstractDbtBase) -> str | None:
    """Determine if profile uses mapping or YAML file."""
    if hasattr(operator, "profile_config") and operator.profile_config:
        profile_config = operator.profile_config
        if profile_config.profile_mapping:
            return "profile_mapping"
        elif profile_config.profiles_yml_filepath:
            return "yaml_file"
    return None


def _get_profile_mapping_class(operator: AbstractDbtBase) -> str | None:
    """Get the profile mapping class name if using profile mapping."""
    if hasattr(operator, "profile_config") and operator.profile_config:
        profile_config = operator.profile_config
        if profile_config.profile_mapping:
            return profile_config.profile_mapping.__class__.__name__
    return None


def _get_task_origin(dag: DAG | None, task_group: TaskGroup | None) -> str:
    """Determine if task belongs to DbtDag, DbtTaskGroup, or standalone."""
    if task_group:
        # Check if task_group is a DbtTaskGroup
        from cosmos.airflow.task_group import DbtTaskGroup

        if isinstance(task_group, DbtTaskGroup):
            return "task_group"

    if dag:
        # Check if dag is a DbtDag
        from cosmos.airflow.dag import DbtDag

        if isinstance(dag, DbtDag):
            return "dbt_dag"

    return "task"


def _get_invocation_mode(operator: AbstractDbtBase, execution_mode: str) -> str | None:
    """Get invocation mode for local execution mode."""
    if execution_mode != "local":
        return None

    # Check if operator has invocation_mode attribute
    if hasattr(operator, "invocation_mode"):
        invocation_mode = operator.invocation_mode
        if hasattr(invocation_mode, "value"):
            return invocation_mode.value
        return str(invocation_mode)

    # Check if there's an execution_config with invocation_mode
    if hasattr(operator, "execution_config") and operator.execution_config:
        if hasattr(operator.execution_config, "invocation_mode"):
            invocation_mode = operator.execution_config.invocation_mode
            if hasattr(invocation_mode, "value"):
                return invocation_mode.value
            return str(invocation_mode)

    # Default to subprocess if we can't determine
    # This could be enhanced by checking if dbtRunner is actually being used
    return "subprocess"


# Public API functions for emitting metrics


def emit_operator_metric(operator: AbstractDbtBase, metric_type: MetricType = MetricType.COUNTER) -> None:
    """Emit metric for which operator was used."""
    operator_name = _get_operator_class_name(operator)
    metric = TelemetryMetric(
        name=f"cosmos.task.operator.{operator_name}",
        metric_type=metric_type,
        tags={"operator": operator_name},
    )
    emit_metric(metric)


def emit_subclass_metric(operator: AbstractDbtBase) -> None:
    """Emit metric indicating if operator was subclassed."""
    is_subclass = _is_subclassed_operator(operator)
    if is_subclass:
        metric = TelemetryMetric(
            name="cosmos.task.operator.is_subclass",
            metric_type=MetricType.COUNTER,
        )
        emit_metric(metric)


def emit_dbt_command_metric(operator: AbstractDbtBase) -> None:
    """Emit metric for which dbt command was used."""
    dbt_command = _get_dbt_command(operator)
    metric = TelemetryMetric(
        name=f"cosmos.task.operator_command.{dbt_command}",
        metric_type=MetricType.COUNTER,
        tags={"dbt_command": dbt_command},
    )
    emit_metric(metric)


def emit_execution_mode_metric(operator: AbstractDbtBase) -> None:
    """Emit metric for which execution mode was used."""
    execution_mode = _get_execution_mode(operator)
    metric = TelemetryMetric(
        name=f"cosmos.task.execution_mode.{execution_mode}",
        metric_type=MetricType.COUNTER,
        tags={"execution_mode": execution_mode},
    )
    emit_metric(metric)


def emit_database_metric(operator: AbstractDbtBase) -> None:
    """Emit metric for which database/profile type was used."""
    profile_type = _get_profile_type(operator)
    if profile_type:
        metric = TelemetryMetric(
            name=f"cosmos.task.database.{profile_type}",
            metric_type=MetricType.COUNTER,
            tags={"profile_type": profile_type},
        )
        emit_metric(metric)


def emit_profile_method_metrics(operator: AbstractDbtBase) -> None:
    """Emit metrics for how profile was defined (mapping vs YAML)."""
    profile_method = _get_profile_method(operator)
    if profile_method:
        metric = TelemetryMetric(
            name=f"cosmos.task.profile.{profile_method}",
            metric_type=MetricType.COUNTER,
            tags={"profile_method": profile_method},
        )
        emit_metric(metric)

        # If using profile mapping, also emit the specific mapping class
        if profile_method == "profile_mapping":
            mapping_class = _get_profile_mapping_class(operator)
            if mapping_class:
                metric = TelemetryMetric(
                    name=f"cosmos.task.profile.{mapping_class}",
                    metric_type=MetricType.COUNTER,
                    tags={"profile_mapping_class": mapping_class},
                )
                emit_metric(metric)


def emit_task_origin_metric(dag: DAG | None, task_group: TaskGroup | None) -> None:
    """Emit metric for task origin (DbtDag, DbtTaskGroup, or standalone task)."""
    origin = _get_task_origin(dag, task_group)
    metric = TelemetryMetric(
        name=f"cosmos.task.origin.{origin}",
        metric_type=MetricType.COUNTER,
        tags={"origin": origin},
    )
    emit_metric(metric)


def emit_task_status_metric(success: bool) -> None:
    """Emit metric for task success or failure."""
    status = "success" if success else "failure"
    metric = TelemetryMetric(
        name=f"cosmos.task.status.{status}",
        metric_type=MetricType.COUNTER,
        tags={"status": status},
    )
    emit_metric(metric)


def emit_local_invocation_mode_metric(operator: AbstractDbtBase, execution_mode: str) -> None:
    """Emit metric for local execution mode invocation (subprocess vs dbt_runner)."""
    if execution_mode == "local":
        invocation_mode = _get_invocation_mode(operator, execution_mode)
        if invocation_mode:
            metric = TelemetryMetric(
                name=f"cosmos.task.execution_mode.local.invocation_mode.{invocation_mode}",
                metric_type=MetricType.COUNTER,
                tags={"invocation_mode": invocation_mode},
            )
            emit_metric(metric)


def emit_load_mode_metric(load_mode: str) -> None:
    """Emit metric for dbt parsing load mode."""
    metric = TelemetryMetric(
        name=f"cosmos.dbt_parsing.load_mode.{load_mode}",
        metric_type=MetricType.COUNTER,
        tags={"load_mode": load_mode},
    )
    emit_metric(metric)


def emit_dbt_ls_invocation_mode_metric(load_mode: str, invocation_mode: str) -> None:
    """Emit metric for dbt_ls load mode invocation (subprocess vs dbt_runner)."""
    if load_mode == "dbt_ls":
        metric = TelemetryMetric(
            name=f"cosmos.task.load_mode.dbt_ls.invocation_mode.{invocation_mode}",
            metric_type=MetricType.COUNTER,
            tags={"invocation_mode": invocation_mode},
        )
        emit_metric(metric)


def emit_parsing_duration_metric(load_mode: str, duration_ms: float) -> None:
    """Emit timing metric for DAG parsing duration."""
    metric = TelemetryMetric(
        name=f"cosmos.dbt_parsing.load_mode.{load_mode}.duration",
        metric_type=MetricType.TIMER,
        value=duration_ms,
        tags={"load_mode": load_mode},
    )
    emit_metric(metric)


def emit_project_size_metric(load_mode: str, model_count: int) -> None:
    """Emit gauge metric for dbt project size (number of models)."""
    metric = TelemetryMetric(
        name=f"cosmos.dbt_parsing.load_mode.{load_mode}.dbt_project_size",
        metric_type=MetricType.GAUGE,
        value=float(model_count),
        tags={"load_mode": load_mode},
    )
    emit_metric(metric)


def emit_task_duration_metric(operator: AbstractDbtBase, duration_ms: float) -> None:
    """Emit timing metric for task duration."""
    operator_name = _get_operator_class_name(operator)
    metric = TelemetryMetric(
        name=f"cosmos.task.operator.{operator_name}",
        metric_type=MetricType.TIMER,
        value=duration_ms,
        tags={"operator": operator_name},
    )
    emit_metric(metric)


def collect_task_metrics(
    operator: AbstractDbtBase, dag: DAG | None = None, task_group: TaskGroup | None = None
) -> None:
    """Collect all task-level metrics for an operator.

    This function should be called during task execution to collect comprehensive
    telemetry about the operator, its configuration, and execution context.

    Args:
        operator: The Cosmos operator instance
        dag: The Airflow DAG (optional, for determining task origin)
        task_group: The Airflow TaskGroup (optional, for determining task origin)
    """
    try:
        # Operator and command metrics
        emit_operator_metric(operator)
        emit_subclass_metric(operator)
        emit_dbt_command_metric(operator)
        emit_execution_mode_metric(operator)

        # Profile metrics
        emit_database_metric(operator)
        emit_profile_method_metrics(operator)

        # Task origin
        emit_task_origin_metric(dag, task_group)

        # Invocation mode for local execution
        execution_mode = _get_execution_mode(operator)
        emit_local_invocation_mode_metric(operator, execution_mode)
    except Exception:
        # Silently fail - telemetry should never break task execution
        pass
