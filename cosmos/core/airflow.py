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
        models: dict,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        # TODO: handle this
        # if the user doesn't specify a dag_id, use the entity id
        # if "dag_id" not in kwargs:
        #     kwargs["dag_id"] = cosmos_group.id

        super().__init__(*args, **kwargs)

        # loop through the models and create a group for each one
        # TODO: doesn't need to be a dict
        for model in models.values():
            model_task_group = TaskGroup(group_id=model.name, dag=self)
            # create a task for each model
            model_run_task = get_airflow_task(
                task=model.task,
                dag=self,
                task_group=model_task_group,
            )
            # create a task group if there are tests
            if model.tests:
                test_task_group = TaskGroup(
                    group_id=f"{model.name}_tests",
                    dag=self,
                )
                # create a task for each test
                for test in model.tests:
                    test_task = get_airflow_task(
                        task=test.task,
                        dag=self,
                        task_group=test_task_group,
                    )
                    # create dependencies between the model and the tests
                    model_run_task >> test_task

                    # create downstream dependencies
                    for child in model.child_models:
                        test_task >> child.task
            else:
                # create downstream dependencies
                for child in model.child_models:
                    model_run_task >> child.task


# TODO: handle this
class CosmosTaskGroup(TaskGroup):
    """
    Render a Group as an Airflow TaskGroup. Subclass of Airflow TaskGroup.
    """

    def __init__(
        self,
        cosmos_group: Group,
        dag: Optional[DAG] = None,
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
                entities[ent.id] = CosmosTaskGroup(cosmos_group=ent, dag=dag, parent_group=self)
            elif isinstance(ent, Task):
                entities[ent.id] = get_airflow_task(
                    task=ent,
                    dag=dag,
                    task_group=self,
                )

        # add dependencies
        for ent in cosmos_group.entities:
            for upstream_id in ent.upstream_entity_ids:
                if upstream_id not in entities:
                    raise ValueError(f"Entity {upstream_id} is not in the group {cosmos_group.id}")

                if ent.id not in entities:
                    raise ValueError(f"Entity {ent.id} is not in the group {cosmos_group.id}")

                entities[upstream_id] >> entities[ent.id]


def get_airflow_task(task: Task, dag: DAG, task_group: Optional[TaskGroup] = None) -> BaseOperator:
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
        raise TypeError(f"Operator class {task.operator_class} is not a subclass of BaseOperator")

    return airflow_task
