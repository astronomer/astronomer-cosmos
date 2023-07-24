from __future__ import annotations

import logging
from typing import Any, Callable

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos.constants import DbtResourceType, TestBehavior, ExecutionMode
from cosmos.core.airflow import get_airflow_task as create_airflow_task
from cosmos.core.graph.entities import Task as TaskMetadata
from cosmos.dataset import get_dbt_dataset
from cosmos.dbt.graph import DbtNode


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
    args = {**args, **{"models": node.name}}

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
    else:
        logger.error(f"Unsupported resource type {node.resource_type} (node {node.unique_id}).")
        return None


def create_test_task_metadata(
    test_task_name: str,
    execution_mode: ExecutionMode,
    task_args: dict[str, Any],
    on_warning_callback: Callable[..., Any] | None = None,
    model_name: str | None = None,
) -> TaskMetadata:
    """
    Create the metadata that will be used to instantiate the Airflow Task that will be used to run the Dbt test node.

    :param test_task_name: Name of the Airflow task to be created
    :param execution_mode: The Cosmos execution mode we're aiming to run the dbt task at (e.g. local)
    :param task_args: Arguments to be used to instantiate an Airflow Task
    :param on_warning_callback: A callback function called on warnings with additional Context variables “test_names”
    and “test_results” of type List.
    :param model_name: If the test relates to a specific model, the name of the model it relates to
    :returns: The metadata necessary to instantiate the source dbt node as an Airflow task.
    """
    task_args = dict(task_args)
    task_args["on_warning_callback"] = on_warning_callback
    if model_name is not None:
        task_args["models"] = model_name
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
    execution_mode: ExecutionMode,  # Cosmos-specific - decide what which class to use
    task_args: dict[str, Any],  # Cosmos/DBT - used to instantiate tasks
    test_behavior: TestBehavior,  # Cosmos-specific: how to inject tests to Airflow DAG
    dbt_project_name: str,  # DBT / Cosmos - used to name test task if mode is after_all,
    conn_id: str,  # Cosmos, dataset URI
    task_group: TaskGroup | None = None,
    on_warning_callback: Callable[..., Any] | None = None,  # argument specific to the DBT test command
    emit_datasets: bool = True,  # Cosmos
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
    :param execution_mode: Where Cosmos should run each dbt task (e.g. ExecutionMode.LOCAL, ExecutionMode.KUBERNETES).
        Default is ExecutionMode.LOCAL.
    :param task_args: Arguments to be used to instantiate an Airflow Task
    :param test_behavior: When to run `dbt` tests. Default is TestBehavior.AFTER_EACH, that runs tests after each model.
    :param dbt_project_name: Name of the dbt pipeline of interest
    :param conn_id: Airflow connection ID
    :param task_group: Airflow Task Group instance
    :param on_warning_callback: A callback function called on warnings with additional Context variables “test_names”
    and “test_results” of type List.
    :param emit_datasets: Decides if Cosmos should add outlets to model classes or not.
    """
    tasks_map = {}

    # In most cases, we'll  map one DBT node to one Airflow task
    # The exception are the test nodes, since it would be too slow to run test tasks individually.
    # If test_behaviour=="after_each", each model task will be bundled with a test task, using TaskGroup
    for node_id, node in nodes.items():
        task_meta = create_task_metadata(node=node, execution_mode=execution_mode, args=task_args)
        if emit_datasets:
            task_args["outlets"] = [get_dbt_dataset(conn_id, dbt_project_name, node.name)]
        if task_meta and node.resource_type != DbtResourceType.TEST:
            if node.resource_type == DbtResourceType.MODEL and test_behavior == TestBehavior.AFTER_EACH:
                with TaskGroup(dag=dag, group_id=node.name, parent_group=task_group) as model_task_group:
                    task = create_airflow_task(task_meta, dag, task_group=model_task_group)
                    test_meta = create_test_task_metadata(
                        f"{node.name}_test",
                        execution_mode,
                        task_args=task_args,
                        model_name=node.name,
                        on_warning_callback=on_warning_callback,
                    )
                    test_task = create_airflow_task(test_meta, dag, task_group=model_task_group)
                    task >> test_task
                    task_or_group = model_task_group
            else:
                task_or_group = create_airflow_task(task_meta, dag, task_group=task_group)
            tasks_map[node_id] = task_or_group

    # If test_behaviour=="after_all", there will be one test task, run "by the end" of the DAG
    # The end of a DAG is defined by the DAG leaf tasks (tasks which do not have downstream tasks)
    if test_behavior == TestBehavior.AFTER_ALL:
        task_args.pop("outlets", None)
        test_meta = create_test_task_metadata(
            f"{dbt_project_name}_test", execution_mode, task_args=task_args, on_warning_callback=on_warning_callback
        )
        test_task = create_airflow_task(test_meta, dag, task_group=task_group)
        leaves_ids = calculate_leaves(tasks_ids=list(tasks_map.keys()), nodes=nodes)
        for leaf_node_id in leaves_ids:
            tasks_map[leaf_node_id] >> test_task

    # Create the Airflow task dependencies between non-test nodes
    for node_id, node in nodes.items():
        for parent_node_id in node.depends_on:
            # depending on the node type, it will not have mapped 1:1 to tasks_map
            if (node_id in tasks_map) and (parent_node_id in tasks_map):
                tasks_map[parent_node_id] >> tasks_map[node_id]
