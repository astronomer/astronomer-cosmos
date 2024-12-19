from __future__ import annotations

from typing import Any, Callable, Union

from airflow.models import BaseOperator
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos.config import RenderConfig
from cosmos.constants import (
    DBT_COMPILE_TASK_ID,
    DEFAULT_DBT_RESOURCES,
    SUPPORTED_BUILD_RESOURCES,
    TESTABLE_DBT_RESOURCES,
    DbtResourceType,
    ExecutionMode,
    SourceRenderingBehavior,
    TestBehavior,
    TestIndirectSelection,
)
from cosmos.core.airflow import get_airflow_task as create_airflow_task
from cosmos.core.graph.entities import Task as TaskMetadata
from cosmos.dbt.graph import DbtNode
from cosmos.log import get_logger

logger = get_logger(__name__)


def _snake_case_to_camelcase(value: str) -> str:
    """Convert snake_case to CamelCase

    Example: foo_bar_baz -> FooBarBaz

    :param value: Value to convert to CamelCase
    :return: Converted value
    """
    return "".join(x.capitalize() for x in value.lower().split("_"))


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
    return (
        f"cosmos.operators.{execution_mode.value}.{dbt_class}{_snake_case_to_camelcase(execution_mode.value)}Operator"
    )


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


def create_test_task_metadata(
    test_task_name: str,
    execution_mode: ExecutionMode,
    test_indirect_selection: TestIndirectSelection,
    task_args: dict[str, Any],
    on_warning_callback: Callable[..., Any] | None = None,
    node: DbtNode | None = None,
    render_config: RenderConfig | None = None,
) -> TaskMetadata:
    """
    Create the metadata that will be used to instantiate the Airflow Task that will be used to run the Dbt test node.

    :param test_task_name: Name of the Airflow task to be created
    :param execution_mode: The Cosmos execution mode we're aiming to run the dbt task at (e.g. local)
    :param task_args: Arguments to be used to instantiate an Airflow Task
    :param on_warning_callback: A callback function called on warnings with additional Context variables “test_names”
    and “test_results” of type List.
    :param node: If the test relates to a specific node, the node reference
    :returns: The metadata necessary to instantiate the source dbt node as an Airflow task.
    """
    task_args = dict(task_args)
    task_args["on_warning_callback"] = on_warning_callback
    extra_context = {}

    task_owner = ""
    airflow_task_config = {}
    if test_indirect_selection != TestIndirectSelection.EAGER:
        task_args["indirect_selection"] = test_indirect_selection.value
    if node is not None:
        if node.resource_type == DbtResourceType.MODEL:
            task_args["models"] = node.resource_name
        elif node.resource_type == DbtResourceType.SOURCE:
            task_args["select"] = f"source:{node.resource_name}"
        else:  # tested with node.resource_type == DbtResourceType.SEED or DbtResourceType.SNAPSHOT
            task_args["select"] = node.resource_name

        extra_context = {"dbt_node_config": node.context_dict}
        task_owner = node.owner
        airflow_task_config = node.airflow_task_config

    elif render_config is not None:  # TestBehavior.AFTER_ALL
        task_args["select"] = render_config.select
        task_args["selector"] = render_config.selector
        task_args["exclude"] = render_config.exclude

    return TaskMetadata(
        id=test_task_name,
        owner=task_owner,
        airflow_task_config=airflow_task_config,
        operator_class=calculate_operator_class(
            execution_mode=execution_mode,
            dbt_class="DbtTest",
        ),
        arguments=task_args,
        extra_context=extra_context,
    )


def _get_task_id_and_args(
    node: DbtNode,
    args: dict[str, Any],
    use_task_group: bool,
    normalize_task_id: Callable[..., Any] | None,
    resource_suffix: str,
) -> tuple[str, dict[str, Any]]:
    """
    Generate task ID and update args with display name if needed.
    """
    args_update = args
    if use_task_group:
        task_id = resource_suffix
    elif normalize_task_id:
        task_id = normalize_task_id(node)
        args_update["task_display_name"] = f"{node.name}_{resource_suffix}"
    else:
        task_id = f"{node.name}_{resource_suffix}"
    return task_id, args_update


def create_dbt_resource_to_class(test_behavior: TestBehavior) -> dict[str, str]:
    """
    Return the map from dbt node type to Cosmos class prefix that should be used
    to handle them.
    """

    if test_behavior == TestBehavior.BUILD:
        dbt_resource_to_class = {
            DbtResourceType.MODEL: "DbtBuild",
            DbtResourceType.SNAPSHOT: "DbtBuild",
            DbtResourceType.SEED: "DbtBuild",
            DbtResourceType.TEST: "DbtTest",
            DbtResourceType.SOURCE: "DbtSource",
        }
    else:
        dbt_resource_to_class = {
            DbtResourceType.MODEL: "DbtRun",
            DbtResourceType.SNAPSHOT: "DbtSnapshot",
            DbtResourceType.SEED: "DbtSeed",
            DbtResourceType.TEST: "DbtTest",
            DbtResourceType.SOURCE: "DbtSource",
        }
    return dbt_resource_to_class


def create_task_metadata(
    node: DbtNode,
    execution_mode: ExecutionMode,
    args: dict[str, Any],
    dbt_dag_task_group_identifier: str,
    use_task_group: bool = False,
    source_rendering_behavior: SourceRenderingBehavior = SourceRenderingBehavior.NONE,
    normalize_task_id: Callable[..., Any] | None = None,
    test_behavior: TestBehavior = TestBehavior.AFTER_ALL,
    on_warning_callback: Callable[..., Any] | None = None,
) -> TaskMetadata | None:
    """
    Create the metadata that will be used to instantiate the Airflow Task used to run the Dbt node.

    :param node: The dbt node which we desired to convert into an Airflow Task
    :param execution_mode: Where Cosmos should run each dbt task (e.g. ExecutionMode.LOCAL, ExecutionMode.KUBERNETES).
         Default is ExecutionMode.LOCAL.
    :param args: Arguments to be used to instantiate an Airflow Task
    :param dbt_dag_task_group_identifier: Identifier to refer to the DbtDAG or DbtTaskGroup in the DAG.
    :param use_task_group: It determines whether to use the name as a prefix for the task id or not.
        If it is False, then use the name as a prefix for the task id, otherwise do not.
    :param on_warning_callback: A callback function called on warnings with additional Context variables “test_names”
        and “test_results” of type List. This is param available for dbt test and dbt source freshness command.
    :returns: The metadata necessary to instantiate the source dbt node as an Airflow task.
    """
    dbt_resource_to_class = create_dbt_resource_to_class(test_behavior)

    args = {**args, **{"models": node.resource_name}}

    if DbtResourceType(node.resource_type) in DEFAULT_DBT_RESOURCES and node.resource_type in dbt_resource_to_class:
        extra_context: dict[str, Any] = {
            "dbt_node_config": node.context_dict,
            "dbt_dag_task_group_identifier": dbt_dag_task_group_identifier,
        }

        if test_behavior == TestBehavior.BUILD and node.resource_type in SUPPORTED_BUILD_RESOURCES:
            task_id = f"{node.name}_{node.resource_type.value}_build"
        elif node.resource_type == DbtResourceType.MODEL:
            task_id, args = _get_task_id_and_args(node, args, use_task_group, normalize_task_id, "run")
        elif node.resource_type == DbtResourceType.SOURCE:
            args["on_warning_callback"] = on_warning_callback

            if (source_rendering_behavior == SourceRenderingBehavior.NONE) or (
                source_rendering_behavior == SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS
                and node.has_freshness is False
                and node.has_test is False
            ):
                return None
            args["select"] = f"source:{node.resource_name}"
            args.pop("models")
            task_id, args = _get_task_id_and_args(node, args, use_task_group, normalize_task_id, "source")
            if node.has_freshness is False and source_rendering_behavior == SourceRenderingBehavior.ALL:
                # render sources without freshness as empty operators
                # empty operator does not accept custom parameters (e.g., profile_args). recreate the args.
                if "task_display_name" in args:
                    args = {"task_display_name": args["task_display_name"]}
                else:
                    args = {}
                return TaskMetadata(id=task_id, operator_class="airflow.operators.empty.EmptyOperator", arguments=args)
        else:
            task_id, args = _get_task_id_and_args(
                node, args, use_task_group, normalize_task_id, node.resource_type.value
            )

        task_metadata = TaskMetadata(
            id=task_id,
            owner=node.owner,
            airflow_task_config=node.airflow_task_config,
            operator_class=calculate_operator_class(
                execution_mode=execution_mode, dbt_class=dbt_resource_to_class[node.resource_type]
            ),
            arguments=args,
            extra_context=extra_context,
        )
        return task_metadata
    else:
        msg = (
            f"Unavailable conversion function for <{node.resource_type}> (node <{node.unique_id}>). "
            "Define a converter function using render_config.node_converters."
        )
        logger.warning(msg)
        return None


def generate_task_or_group(
    dag: DAG,
    task_group: TaskGroup | None,
    node: DbtNode,
    execution_mode: ExecutionMode,
    task_args: dict[str, Any],
    test_behavior: TestBehavior,
    source_rendering_behavior: SourceRenderingBehavior,
    test_indirect_selection: TestIndirectSelection,
    on_warning_callback: Callable[..., Any] | None,
    normalize_task_id: Callable[..., Any] | None = None,
    **kwargs: Any,
) -> BaseOperator | TaskGroup | None:
    task_or_group: BaseOperator | TaskGroup | None = None

    use_task_group = (
        node.resource_type in TESTABLE_DBT_RESOURCES
        and test_behavior == TestBehavior.AFTER_EACH
        and node.has_test is True
    )

    task_meta = create_task_metadata(
        node=node,
        execution_mode=execution_mode,
        args=task_args,
        dbt_dag_task_group_identifier=_get_dbt_dag_task_group_identifier(dag, task_group),
        use_task_group=use_task_group,
        source_rendering_behavior=source_rendering_behavior,
        normalize_task_id=normalize_task_id,
        test_behavior=test_behavior,
        on_warning_callback=on_warning_callback,
    )

    # In most cases, we'll  map one DBT node to one Airflow task
    # The exception are the test nodes, since it would be too slow to run test tasks individually.
    # If test_behaviour=="after_each", each model task will be bundled with a test task, using TaskGroup
    if task_meta and node.resource_type != DbtResourceType.TEST:
        if use_task_group:
            with TaskGroup(dag=dag, group_id=node.name, parent_group=task_group) as model_task_group:
                task = create_airflow_task(task_meta, dag, task_group=model_task_group)
                test_meta = create_test_task_metadata(
                    "test",
                    execution_mode,
                    test_indirect_selection,
                    task_args=task_args,
                    node=node,
                    on_warning_callback=on_warning_callback,
                )
                test_task = create_airflow_task(test_meta, dag, task_group=model_task_group)
                task >> test_task
                task_or_group = model_task_group
        else:
            task_or_group = create_airflow_task(task_meta, dag, task_group=task_group)
    return task_or_group


def _add_dbt_compile_task(
    nodes: dict[str, DbtNode],
    dag: DAG,
    execution_mode: ExecutionMode,
    task_args: dict[str, Any],
    tasks_map: dict[str, Any],
    task_group: TaskGroup | None,
) -> None:
    if execution_mode != ExecutionMode.AIRFLOW_ASYNC:
        return

    compile_task_metadata = TaskMetadata(
        id=DBT_COMPILE_TASK_ID,
        operator_class="cosmos.operators.airflow_async.DbtCompileAirflowAsyncOperator",
        arguments=task_args,
        extra_context={"dbt_dag_task_group_identifier": _get_dbt_dag_task_group_identifier(dag, task_group)},
    )
    compile_airflow_task = create_airflow_task(compile_task_metadata, dag, task_group=task_group)

    for task_id, task in tasks_map.items():
        if not task.upstream_list:
            compile_airflow_task >> task

    tasks_map[DBT_COMPILE_TASK_ID] = compile_airflow_task


def _get_dbt_dag_task_group_identifier(dag: DAG, task_group: TaskGroup | None) -> str:
    dag_id = dag.dag_id
    task_group_id = task_group.group_id if task_group else None
    identifiers_list = []
    if dag_id:
        identifiers_list.append(dag_id)
    if task_group_id:
        identifiers_list.append(task_group_id)
    dag_task_group_identifier = "__".join(identifiers_list)

    return dag_task_group_identifier


def build_airflow_graph(
    nodes: dict[str, DbtNode],
    dag: DAG,  # Airflow-specific - parent DAG where to associate tasks and (optional) task groups
    execution_mode: ExecutionMode,  # Cosmos-specific - decide what which class to use
    task_args: dict[str, Any],  # Cosmos/DBT - used to instantiate tasks
    test_indirect_selection: TestIndirectSelection,  # Cosmos/DBT - used to set test indirect selection mode
    dbt_project_name: str,  # DBT / Cosmos - used to name test task if mode is after_all,
    render_config: RenderConfig,
    task_group: TaskGroup | None = None,
    on_warning_callback: Callable[..., Any] | None = None,  # argument specific to the DBT test command
) -> dict[str, Union[TaskGroup, BaseOperator]]:
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

    :param nodes: Dictionary mapping dbt nodes (node.unique_id to node)
    :param dag: Airflow DAG instance
    :param execution_mode: Where Cosmos should run each dbt task (e.g. ExecutionMode.LOCAL, ExecutionMode.KUBERNETES).
        Default is ExecutionMode.LOCAL.
    :param task_args: Arguments to be used to instantiate an Airflow Task
    :param dbt_project_name: Name of the dbt pipeline of interest
    :param task_group: Airflow Task Group instance
    :param on_warning_callback: A callback function called on warnings with additional Context variables “test_names”
    and “test_results” of type List.
    :return: Dictionary mapping dbt nodes (node.unique_id to Airflow task)
    """
    node_converters = render_config.node_converters or {}
    test_behavior = render_config.test_behavior
    source_rendering_behavior = render_config.source_rendering_behavior
    normalize_task_id = render_config.normalize_task_id
    tasks_map: dict[str, Union[TaskGroup, BaseOperator]] = {}
    task_or_group: TaskGroup | BaseOperator

    for node_id, node in nodes.items():
        conversion_function = node_converters.get(node.resource_type, generate_task_or_group)
        if conversion_function != generate_task_or_group:
            logger.warning(
                "The `node_converters` attribute is an experimental feature. "
                "Its syntax and behavior can be changed before a major release."
            )
        logger.debug(f"Converting <{node.unique_id}> using <{conversion_function.__name__}>")
        task_or_group = conversion_function(  # type: ignore
            dag=dag,
            task_group=task_group,
            dbt_project_name=dbt_project_name,
            execution_mode=execution_mode,
            task_args=task_args,
            test_behavior=test_behavior,
            source_rendering_behavior=source_rendering_behavior,
            test_indirect_selection=test_indirect_selection,
            on_warning_callback=on_warning_callback,
            normalize_task_id=normalize_task_id,
            node=node,
        )
        if task_or_group is not None:
            logger.debug(f"Conversion of <{node.unique_id}> was successful!")
            tasks_map[node_id] = task_or_group

    # If test_behaviour=="after_all", there will be one test task, run by the end of the DAG
    # The end of a DAG is defined by the DAG leaf tasks (tasks which do not have downstream tasks)
    if test_behavior == TestBehavior.AFTER_ALL:
        test_meta = create_test_task_metadata(
            f"{dbt_project_name}_test",
            execution_mode,
            test_indirect_selection,
            task_args=task_args,
            on_warning_callback=on_warning_callback,
            render_config=render_config,
        )
        test_task = create_airflow_task(test_meta, dag, task_group=task_group)
        leaves_ids = calculate_leaves(tasks_ids=list(tasks_map.keys()), nodes=nodes)
        for leaf_node_id in leaves_ids:
            tasks_map[leaf_node_id] >> test_task

    create_airflow_task_dependencies(nodes, tasks_map)
    _add_dbt_compile_task(nodes, dag, execution_mode, task_args, tasks_map, task_group)
    return tasks_map


def create_airflow_task_dependencies(
    nodes: dict[str, DbtNode],
    tasks_map: dict[str, Union[TaskGroup, BaseOperator]],
) -> None:
    """
    Create the Airflow task dependencies between non-test nodes.
    :param nodes: Dictionary mapping dbt nodes (node.unique_id to node)
    :param tasks_map: Dictionary mapping dbt nodes (node.unique_id to Airflow task)
    """
    for node_id, node in nodes.items():
        for parent_node_id in node.depends_on:
            # depending on the node type, it will not have mapped 1:1 to tasks_map
            if (node_id in tasks_map) and (parent_node_id in tasks_map):
                tasks_map[parent_node_id] >> tasks_map[node_id]
