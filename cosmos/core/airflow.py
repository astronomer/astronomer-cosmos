import importlib
import logging
from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos.core.graph.entities import Group, Task

logger = logging.getLogger(__name__)


class CosmosDag(DAG):
    """
    Render a Group as an Airflow DAG. Subclass of Airflow DAG.
    """

    def __init__(
        self,
        cosmos_group: Group,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        # if the user doesn't specify a dag_id, use the entity id
        if "dag_id" not in kwargs:
            kwargs["dag_id"] = cosmos_group.id

        super().__init__(*args, **kwargs)

        entities: Dict[str, Any] = {}

        # render all the entities in the group
        for ent in cosmos_group.entities:
            if isinstance(ent, Group):
                entities[ent.id] = CosmosTaskGroup(cosmos_group=ent, dag=self)
            elif isinstance(ent, Task):
                entities[ent.id] = get_airflow_task(task=ent, dag=self)

        # add dependencies
        for ent in cosmos_group.entities:
            for upstream_id in ent.upstream_entity_ids:
                entities[upstream_id] >> entities[ent.id]


class CosmosTaskGroup(TaskGroup):
    """
    Render a Group as an Airflow TaskGroup. Subclass of Airflow TaskGroup.
    """

    def __init__(
        self,
        cosmos_group: Group,
        dag: DAG,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        # if the user doesn't specify a group_id, use the entity id
        if "group_id" not in kwargs:
            kwargs["group_id"] = cosmos_group.id

        # add dag back to kwargs and call the airflow constructor
        kwargs["dag"] = dag
        super().__init__(*args, **kwargs)

        entities: Dict[str, Any] = {}

        # render all the entities in the group
        for ent in cosmos_group.entities:
            if isinstance(ent, Group):
                entities[ent.id] = CosmosTaskGroup(
                    cosmos_group=ent, dag=dag, parent_group=self
                )
            elif isinstance(ent, Task):
                entities[ent.id] = get_airflow_task(
                    task=ent,
                    dag=dag,
                    task_group=self,
                )

        # add dependencies
        for ent in cosmos_group.entities:
            for upstream_id in ent.upstream_entity_ids:
                entities[upstream_id] >> entities[ent.id]


def get_airflow_task(
    task: Task, dag: DAG, task_group: Optional[TaskGroup] = None
) -> BaseOperator:
    """
    Get the Airflow Operator class for a Task.

    :param task: The Task to get the Operator for

    :return: The Operator class
    :rtype: BaseOperator
    """
    # first, import the operator class from the
    # fully qualified name defined in the task
    module_name, class_name = task.operator_class.rsplit(".", 1)
    module = importlib.import_module(module_name)
    Operator = getattr(module, class_name)

    airflow_task = Operator(
        task_id=task.id,
        dag=dag,
        task_group=task_group,
        **task.arguments,
    )

    if not isinstance(airflow_task, BaseOperator):
        raise TypeError(
            f"Operator class {task.operator_class} is not a subclass of BaseOperator"
        )

    return airflow_task
