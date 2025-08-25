from __future__ import annotations

"""Node converter that uses a single master build task and per-model sensor tasks.

This module is meant to be referenced from ``RenderConfig(node_converters=...)``.
"""

from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.models import DAG

from cosmos.constants import DbtResourceType
from cosmos.dbt.graph import DbtNode
from cosmos.operators.master_build import (
    DbtMasterBuildOperator,
    create_model_result_task,
)

__all__ = ["model_to_sensor_converter"]


def _ensure_master_task(dag: DAG, task_args: Dict[str, Any]) -> DbtMasterBuildOperator:
    """Return the singleton master build task in *dag*, creating it if missing."""
    master_id = "dbt_master_build"
    if master_id in dag.task_ids:
        return dag.get_task(master_id)  # type: ignore[return-value]

    render_config = task_args.get("render_config")
    master_kwargs = {**task_args}
    master_kwargs.update(
        {
            "task_id": master_id,
            "render_config": render_config,
            "dag": dag,
            "retries": 0,
        }
    )
    master = DbtMasterBuildOperator(**master_kwargs)
    return master


def model_to_sensor_converter(
    *,
    dag: DAG,
    node: DbtNode,
    task_args: Dict[str, Any],
    # render_config: Any | None = None,
    **_: Any,
):
    """Converter function for ``DbtResourceType.MODEL`` nodes.

    It ensures there is exactly one heavy :class:`~DbtMasterBuildOperator` and
    for each model node creates a lightweight sensor task that validates the
    model status in ``run_results`` pushed by the master task.
    """
    if node.resource_type != DbtResourceType.MODEL:
        raise AirflowException("This converter only handles model nodes")

    # if "render_config" not in task_args:
    #     task_args["render_config"] = render_config
    master = _ensure_master_task(dag, task_args)

    sensor = create_model_result_task(
        dag=dag,
        model_unique_id=node.unique_id,
        upstream_task_id=master.task_id,
    )

    return sensor
