from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.listeners import hookimpl

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance

from cosmos import telemetry
from cosmos.constants import InvocationMode
from cosmos.log import get_logger
from cosmos.operators.base import AbstractDbtBase

logger = get_logger(__name__)

TASK_INSTANCE_EVENT = "task_instance"


def _is_cosmos_task(task_instance: TaskInstance) -> bool:
    """Return True if the task instance is powered by Cosmos operators."""

    task = task_instance.task
    module = _operator_module(task_instance)
    return module.startswith("cosmos.") or isinstance(task, AbstractDbtBase)


def _execution_mode_from_task(task_instance: TaskInstance) -> str | None:
    """Extract Cosmos execution mode from the task's module path."""

    module = _operator_module(task_instance)
    parts = module.split(".")
    if len(parts) >= 3 and parts[0] == "cosmos" and parts[1] == "operators":
        return parts[2]
    # TODO: When users subclass Cosmos operators in external modules, encode execution mode directly on the task
    # so telemetry does not rely on module inspection.
    return None


def _operator_module(task_instance: TaskInstance) -> str:
    """Return the module path for the operator backing the given task instance."""

    return getattr(task_instance.task, "_task_module", None) or task_instance.task.__class__.__module__


def _is_cosmos_subclass(task_instance: TaskInstance) -> bool:
    """Return True when the task is a custom subclass extending Cosmos operators."""

    return isinstance(task_instance.task, AbstractDbtBase) and not _operator_module(task_instance).startswith("cosmos.")


def _invocation_mode(task_instance: TaskInstance) -> str | None:
    """Return the invocation mode recorded in Cosmos operators."""

    mode = getattr(task_instance.task, "invocation_mode", None)
    if mode is None:
        return None
    if isinstance(mode, InvocationMode):
        return mode.value
    return str(mode)


def _dbt_command(task_instance: TaskInstance) -> str | None:
    """Return the dbt sub-command encoded on Cosmos operators."""

    task = task_instance.task
    if not isinstance(task, AbstractDbtBase):
        return None

    command = getattr(task, "base_cmd", None)
    if command is None:
        return None

    if isinstance(command, (list, tuple)):
        return " ".join(str(part) for part in command if part is not None)

    return str(command)


def _install_deps(task_instance: TaskInstance) -> bool | None:
    """Return the effective install_deps flag when available."""

    task = task_instance.task
    if not isinstance(task, AbstractDbtBase):
        return None

    install_deps = getattr(task, "install_deps", None)
    if install_deps is None:
        return None

    return bool(install_deps)


def _has_callback(task_instance: TaskInstance) -> bool:
    """Return True when a Cosmos operator includes user-defined callbacks."""

    task = task_instance.task
    if not isinstance(task, AbstractDbtBase):
        return False

    callback = getattr(task, "callback", None)
    if callback is None:
        return False

    if isinstance(callback, (list, tuple)):
        return any(callback)

    return bool(callback)


def _build_task_metrics(task_instance: TaskInstance, status: str) -> dict[str, object]:
    """Build telemetry payload for task completion events."""

    metrics: dict[str, object] = {
        "status": status,
        "operator_name": task_instance.task.__class__.__name__,
        "is_cosmos_operator_subclass": _is_cosmos_subclass(task_instance),
        "invocation_mode": _invocation_mode(task_instance),
        "execution_mode": _execution_mode_from_task(task_instance),
        # map_index is -1 for non-mapped tasks, >= 0 for mapped tasks
        "is_mapped_task": task_instance.map_index >= 0,
    }

    dbt_command = _dbt_command(task_instance)
    if dbt_command:
        metrics["dbt_command"] = dbt_command

    install_deps = _install_deps(task_instance)
    if install_deps is not None:
        metrics["install_deps"] = install_deps

    metrics["has_callback"] = _has_callback(task_instance)

    duration = getattr(task_instance, "duration", None)
    if duration is not None:
        metrics["duration"] = duration

    return metrics


@hookimpl
def on_task_instance_success(previous_state: Any, task_instance: TaskInstance, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
    """Handle task instance success for both Airflow 2 (with session) and Airflow 3 (without session)."""
    if not _is_cosmos_task(task_instance):
        return

    logger.debug("Telemetry task listener success for %s.%s", task_instance.dag_id, task_instance.task_id)
    metrics = _build_task_metrics(task_instance, "success")
    telemetry.emit_usage_metrics_if_enabled(TASK_INSTANCE_EVENT, metrics)


@hookimpl
def on_task_instance_failed(previous_state: Any, task_instance: TaskInstance, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
    """Handle task instance failure for both Airflow 2 (with session) and Airflow 3 (with error and without session)."""
    if not _is_cosmos_task(task_instance):
        return

    logger.debug("Telemetry task listener failure for %s.%s", task_instance.dag_id, task_instance.task_id)
    metrics = _build_task_metrics(task_instance, "failed")
    telemetry.emit_usage_metrics_if_enabled(TASK_INSTANCE_EVENT, metrics)
