from __future__ import annotations

from typing import TYPE_CHECKING

from airflow import DAG

if TYPE_CHECKING:
    try:
        # Airflow 3.1 onwards
        from airflow.utils.task_group import TaskGroup
    except ImportError:
        from airflow.utils.task_group import TaskGroup


def get_dataset_alias_name(dag: DAG | None, task_group: TaskGroup | None, task_id: str) -> str:
    """
    Given the Airflow DAG, Airflow TaskGroup and the Airflow Task ID, return the name of the
    Airflow DatasetAlias associated to that task.
    """
    dag_id = None
    task_group_id = None

    if task_group:
        if task_group.dag_id is not None:
            dag_id = task_group.dag_id
        if task_group.group_id is not None:
            task_group_id = task_group.group_id
            task_group_id = task_group_id.split(".")[-1]
    elif dag:
        dag_id = dag.dag_id

    identifiers_list = []

    if dag_id:
        identifiers_list.append(dag_id)
    if task_group_id:
        identifiers_list.append(task_group_id)

    identifiers_list.append(task_id.split(".")[-1])

    return "__".join(identifiers_list)
