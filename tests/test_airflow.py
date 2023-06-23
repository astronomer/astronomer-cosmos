"""Tests for the Airflow integration."""
from __future__ import annotations

from airflow.models.baseoperator import BaseOperator
from airflow.models.taskmixin import DAGNode

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

import pendulum
import pytest
from airflow.models import DAG

from cosmos.core.airflow import CosmosDag, CosmosTaskGroup
from cosmos.core.graph.entities import CosmosEntity, Group, Task


def get_tasks(dag_node: DAGNode) -> list[BaseOperator]:
    """
    Get a list of all tasks in a DAGNode.
    """
    tasks = []
    if isinstance(dag_node, BaseOperator):
        tasks.append(dag_node)
    elif isinstance(dag_node, DAG):
        tasks.extend(dag_node.tasks)
    elif isinstance(dag_node, CosmosTaskGroup):
        for task in dag_node.children.values():
            tasks.extend(get_tasks(task))
    else:
        raise TypeError(f"Unknown type: {type(dag_node)}")

    return tasks


def simple_group() -> Group:
    """
    A Group containing:
    - a single Task
    """
    group = Group(id="simple_group")
    group.add_entity(Task(id="task_1"))

    return group


def nested_group() -> Group:
    """
    A Group containing:
    - a single Task
    - a Group with two Tasks
    """
    group = Group(id="nested_group")
    group.add_entity(Task(id="task_1"))

    sub_group = Group(id="group_2")
    sub_group.add_entity(Task(id="task_2"))
    sub_group.add_entity(Task(id="task_3"))

    group.add_entity(sub_group)

    return group


def nested_group_no_add_entity() -> Group:
    """
    A Group containing:
    - a single Task
    - a Group with two Tasks

    This is the same as nested_group, but uses the `entities` attribute
    instead of the `add_entity` method.
    """
    task_1 = Task(id="task_1")
    task_2 = Task(id="task_2")
    task_3 = Task(id="task_3")

    sub_group = Group(
        id="group_2",
        entities=[task_2, task_3],
    )

    group = Group(
        id="nested_group_no_add_entity",
        entities=[task_1, sub_group],
    )

    return group


def nested_group_with_upstream() -> Group:
    """
    A Group containing:
    - a single Task
    - a Group with two Tasks
    """
    group = Group(id="nested_group_with_upstream")
    task_1 = Task(id="task_1")
    group.add_entity(task_1)

    sub_group = Group(id="group_2")
    task_2 = Task(id="task_2")
    sub_group.add_entity(task_2)
    task_3 = Task(id="task_3")
    sub_group.add_entity(task_3)

    sub_group.add_upstream(task_1)

    group.add_entity(sub_group)

    return group


def double_nested_groups() -> Group:
    """
    A Group containing:
    - a single Task
    - a Group with two Tasks and a nested Group
    """
    group = Group(id="double_nested_groups")
    task_1 = Task(id="task_1")
    group.add_entity(task_1)

    sub_group = Group(id="group_2")
    task_2 = Task(id="task_2")
    sub_group.add_entity(task_2)

    sub_sub_group = Group(id="group_3")
    task_3 = Task(id="task_3")
    sub_sub_group.add_entity(task_3)
    task_4 = Task(id="task_4")
    sub_sub_group.add_entity(task_4)

    sub_group.add_entity(sub_sub_group)

    group.add_entity(sub_group)

    return group


class AirflowTestTask(TypedDict):
    """A test task for Airflow."""

    operator_class: str
    arguments: dict[str, str]
    upstream_entity_ids: list[str]


@pytest.mark.parametrize(
    "group,num_tasks",
    [
        pytest.param(simple_group(), 1, id="simple_group"),
        pytest.param(nested_group(), 3, id="nested_group"),
        pytest.param(
            nested_group_no_add_entity(),
            3,
            id="nested_group_no_add_entity",
        ),
        pytest.param(
            nested_group_with_upstream(),
            3,
            id="nested_group_with_upstream",
        ),
        pytest.param(double_nested_groups(), 4, id="double_nested_groups"),
    ],
)
def test_cosmos_dag_and_task_group(
    group: Group,
    num_tasks: int,
) -> None:
    """
    Tests that the CosmosDag properly renders a Group as an Airflow DAG.
    """
    expected: dict[str, AirflowTestTask] = {}

    def flatten_entities(
        entities: list[CosmosEntity],
        upstream_from_parent: list[str] | None = None,
    ) -> None:
        """
        Flatten a list of CosmosEntities into a list of AirflowTestTasks.
        """
        upstreams = upstream_from_parent or []
        for entity in entities:
            if isinstance(entity, Task):
                expected[entity.id] = {
                    "operator_class": entity.operator_class,
                    "arguments": entity.arguments,
                    "upstream_entity_ids": entity.upstream_entity_ids + upstreams,
                }
            elif isinstance(entity, Group):
                flatten_entities(entity.entities, upstreams + entity.upstream_entity_ids)

    flatten_entities(group.entities)

    ############################
    # CosmosDag
    ############################
    dag = CosmosDag(
        start_date=pendulum.datetime(2021, 1, 1),
        cosmos_group=group,
    )

    # basic dag checks
    assert dag.dag_id == group.id
    assert dag.task_count == num_tasks

    for task in dag.tasks:
        task_id = task.task_id.split(".")[-1]
        cosmos_task = expected[task_id]

        assert task_id in expected

        class_name = str(type(task))
        assert class_name == f"<class '{cosmos_task['operator_class']}'>"

        airflow_upstream_task_ids = list(task.upstream_task_ids)
        assert airflow_upstream_task_ids == cosmos_task["upstream_entity_ids"]

    ############################
    # CosmosTaskGroup
    ############################
    with DAG(dag_id="test", start_date=pendulum.datetime(2021, 1, 1)) as dag:
        task_group = CosmosTaskGroup(
            cosmos_group=group,
        )

    tasks = get_tasks(task_group)

    # basic task group checks
    assert task_group.group_id == group.id
    assert len(tasks) == num_tasks

    for tg_task in tasks:
        task_id = tg_task.task_id.split(".")[-1]
        cosmos_task = expected[task_id]

        assert task_id in expected

        class_name = str(type(tg_task))
        assert class_name == f"<class '{cosmos_task['operator_class']}'>"

        airflow_upstream_task_ids = [id.split(".")[-1] for id in list(tg_task.upstream_task_ids)]
        assert airflow_upstream_task_ids == cosmos_task["upstream_entity_ids"]


def test_invalid_operator() -> None:
    """Tests that an invalid operator raises an error."""
    with pytest.raises(ValueError):
        group = Group(id="group_1")
        group.add_entity(Task(id="task_1", operator_class="InvalidOperator"))

        CosmosDag(
            start_date=pendulum.datetime(2021, 1, 1),
            cosmos_group=group,
        )
