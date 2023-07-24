"Contains functions to build an Airflow DAG from a dbt project."
from __future__ import annotations

import logging
from typing import Any


from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models import BaseOperator

from cosmos.constants import DbtResourceType, TestBehavior, ExecutionMode
from cosmos.core.airflow import get_airflow_task as create_airflow_task
from cosmos.core.graph.entities import Task as TaskMetadata
from cosmos.dataset import get_dbt_dataset
from cosmos.dbt.node import DbtNode
from cosmos.config import CosmosConfig


logger = logging.getLogger(__name__)


def calculate_operator_class(
    execution_mode: ExecutionMode,
    dbt_class: str,
) -> str:
    """
    Given an execution mode and dbt class, return the operator class path to use.

    :param execution_mode: Cosmos execution mode (e.g. local, virtualenv, docker, kubernetes)
    :param dbt_class: The dbt command being used (e.g. DbtSnapshot, DbtRun, DbtSeed)
    :returns: path string to the correspondent Cosmos Airflow operator
    (e.g. cosmos.operators.localDbtSnapshotLocalOperator)
    """
    return f"cosmos.operators.{execution_mode.value}.{dbt_class}{execution_mode.value.capitalize()}Operator"


def calculate_leaves(tasks_ids: list[str], nodes: dict[str, DbtNode]) -> list[str]:
    """
    Return a list of unique_ids for nodes that are not parents (don't have dependencies on other tasks).

    :param tasks_ids: Node/task IDs which are materialized in the Airflow DAG
    :param nodes: Dictionary mapping dbt nodes (node.unique_id to node)
    :returns: List of unique_ids for the nodes that are graph leaves
    """
    parents = []
    leaves = []
    materialized_nodes = [node for node in nodes.values() if node.unique_id in tasks_ids]

    for node in materialized_nodes:
        parents.extend(node.depends_on)

    parents_ids = set(parents)

    for node in materialized_nodes:
        if node.unique_id not in parents_ids:
            leaves.append(node.unique_id)

    return leaves


def create_task_metadata(node: DbtNode, execution_mode: ExecutionMode, args: dict[str, Any]) -> TaskMetadata | None:
    """
    Create the metadata that will be used to instantiate the Airflow Task used to run the Dbt node.

    :param node: The dbt node which we desired to convert into an Airflow Task
    :param execution_mode: Where Cosmos should run each dbt task (e.g. ExecutionMode.LOCAL, ExecutionMode.KUBERNETES).
         Default is ExecutionMode.LOCAL.
    :param args: Arguments to be used to instantiate an Airflow Task
    :returns: The metadata necessary to instantiate the source dbt node as an Airflow task.
    """
    dbt_resource_to_class = {
        DbtResourceType.MODEL: "DbtRun",
        DbtResourceType.SNAPSHOT: "DbtSnapshot",
        DbtResourceType.SEED: "DbtSeed",
        DbtResourceType.TEST: "DbtTest",
    }
    args = {**args, **{"models": [node.name]}}

    if hasattr(node.resource_type, "value") and node.resource_type in dbt_resource_to_class:
        task_id_suffix = "run" if node.resource_type == DbtResourceType.MODEL else node.resource_type.value
        task_metadata = TaskMetadata(
            id=f"{node.name}_{task_id_suffix}",
            operator_class=calculate_operator_class(
                execution_mode=execution_mode, dbt_class=dbt_resource_to_class[node.resource_type]
            ),
            arguments=args,
        )
        return task_metadata

    raise ValueError(f"Unsupported resource type {node.resource_type} (node {node.unique_id}).")


def create_test_task_metadata(
    test_task_name: str,
    execution_mode: ExecutionMode,
    task_args: dict[str, Any],
    model_name: str | None = None,
) -> TaskMetadata:
    """
    Create the metadata that will be used to instantiate the Airflow Task that will be used to run the Dbt test node.

    :param test_task_name: Name of the Airflow task to be created
    :param execution_mode: The Cosmos execution mode we're aiming to run the dbt task at (e.g. local)
    :param task_args: Arguments to be used to instantiate an Airflow Task
    :param model_name: If the test relates to a specific model, the name of the model it relates to
    :returns: The metadata necessary to instantiate the source dbt node as an Airflow task.
    """
    task_args = dict(task_args)

    if model_name is not None:
        task_args["models"] = [model_name]

    return TaskMetadata(
        id=test_task_name,
        operator_class=calculate_operator_class(
            execution_mode=execution_mode,
            dbt_class="DbtTest",
        ),
        arguments=task_args,
    )


def build_airflow_graph(
    nodes: dict[str, DbtNode],
    dag: DAG,  # Airflow-specific - parent DAG where to associate tasks and (optional) task groups
    cosmos_config: CosmosConfig,  # Cosmos - used to render dbt project
    task_args: dict[str, Any],  # Cosmos - used to instantiate tasks
    task_group: TaskGroup | None = None,
) -> None:
    """
    Instantiate dbt `nodes` as Airflow tasks within the given `task_group` (optional) or `dag` (mandatory).

    The following arguments affect how each airflow task is instantiated:
    * `execution_mode`
    * `task_args`

    The parameter `test_behavior` influences how many and where test nodes will be added, while the argument
    `on_warning_callback` allows users to set a callback function to be called depending on the test result.
    If the `test_behavior` is None, no test nodes are added. Otherwise, if the `test_behaviour` is `after_all`,
    a single test task will be added after the Cosmos leave tasks, and it is named using `dbt_project_name`.
    Finally, if the `test_behaviour` is `after_each`, a test will be added after each model.

    If `emit_datasets` is True, tasks will create outlets using:
    * `dbt_project_name`
    * `conn_id`

    :param nodes: Dictionary mapping dbt nodes (node.unique_id to node)
    :param dag: Airflow DAG instance
    :param cosmos_config: Cosmos configuration
    :param task_args: Arguments to be used to instantiate an Airflow Task
    :param task_group: Airflow Task Group instance
    """
    task_args = dict(task_args)
    task_args["cosmos_config"] = cosmos_config

    render_config = cosmos_config.render_config
    profile_config = cosmos_config.profile_config
    project_config = cosmos_config.project_config
    execution_config = cosmos_config.execution_config

    tasks_map: dict[str, BaseOperator | TaskGroup] = {}

    for node_id, node in nodes.items():
        run_metadata = create_task_metadata(
            node=node,
            execution_mode=execution_config.execution_mode_enum,
            args=task_args,
        )

        conn_id = "unknown"
        if profile_config.profile_mapping:
            conn_id = profile_config.profile_mapping.conn_id or "unknown"

        if node.resource_type == DbtResourceType.TEST:
            # TODO: support custom test nodes
            continue

        if node.resource_type == DbtResourceType.MODEL and render_config.test_behavior_enum == TestBehavior.AFTER_EACH:
            # if test_behaviour=="after_each", each model task will be bundled with a test task, using TaskGroup
            test_task_args = dict(task_args)
            if render_config.emit_datasets:
                test_task_args["outlets"] = [get_dbt_dataset(conn_id, project_config.project_name, node.name)]

            test_metadata = create_test_task_metadata(
                f"{node.name}_test",
                execution_mode=execution_config.execution_mode_enum,
                task_args=test_task_args,
                model_name=node.name,
            )

            with TaskGroup(dag=dag, group_id=node.name, parent_group=task_group) as model_task_group:
                run_task = create_airflow_task(run_metadata, dag, task_group=model_task_group)
                test_task = create_airflow_task(test_metadata, dag, task_group=model_task_group)

                run_task >> test_task  # pylint: disable=W0104 # disables the useless statement rule
                tasks_map[node_id] = model_task_group
        else:
            tasks_map[node_id] = create_airflow_task(run_metadata, dag, task_group=task_group)

    # If test_behaviour=="after_all", there will be one test task, run "by the end" of the DAG
    # The end of a DAG is defined by the DAG leaf tasks (tasks which do not have downstream tasks)
    if render_config.test_behavior_enum == TestBehavior.AFTER_ALL:
        task_args.pop("outlets", None)
        test_metadata = create_test_task_metadata(
            f"{project_config.project_name}_test",
            execution_mode=execution_config.execution_mode_enum,
            task_args=task_args,
        )
        test_task = create_airflow_task(test_metadata, dag, task_group=task_group)

        leaves_ids = calculate_leaves(tasks_ids=list(tasks_map.keys()), nodes=nodes)
        for leaf_node_id in leaves_ids:
            tasks_map[leaf_node_id] >> test_task  # pylint: disable=W0104 # disables the useless statement rule

    # Create the Airflow task dependencies between non-test nodes
    for node_id, node in nodes.items():
        for parent_node_id in node.depends_on:
            # depending on the node type, it will not have mapped 1:1 to tasks_map
            if (node_id in tasks_map) and (parent_node_id in tasks_map):
                tasks_map[parent_node_id] >> tasks_map[node_id]
