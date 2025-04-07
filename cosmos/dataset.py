from __future__ import annotations

from typing import Any

from airflow import DAG
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


def configure_datasets(operator_kwargs: dict[str, Any], task_id: str) -> None:
    """
    Sets the outlets for the operator by creating a DatasetAlias.

    :param operator_kwargs: (dict[str, Any])
    :param task_id: (str)
    :return: None
    """
    from airflow.datasets import DatasetAlias

    # ignoring the type because older versions of Airflow raise the follow error in mypy
    # error: Incompatible types in assignment (expression has type "list[DatasetAlias]", target has type "str")
    dag_id = operator_kwargs.get("dag")
    task_group_id = operator_kwargs.get("task_group")
    operator_kwargs["outlets"] = [
        DatasetAlias(name=get_dataset_alias_name(dag_id, task_group_id, task_id))
    ]  # type: ignore
