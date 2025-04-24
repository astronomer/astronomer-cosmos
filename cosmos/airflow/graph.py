from __future__ import annotations

from collections import OrderedDict, defaultdict
from copy import deepcopy
from typing import Any, Callable, Union

try:  # Airflow 3
    from airflow.sdk.bases.operator import BaseOperator
except ImportError:  # Airflow 2
    from airflow.models import BaseOperator

from airflow.models.base import ID_LEN as AIRFLOW_MAX_ID_LENGTH
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos.config import RenderConfig
from cosmos.constants import (
    DBT_SETUP_ASYNC_TASK_ID,
    DBT_TEARDOWN_ASYNC_TASK_ID,
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
from cosmos.exceptions import CosmosValueError
from cosmos.log import get_logger
from cosmos.settings import enable_setup_async_task, enable_teardown_async_task

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


def exclude_detached_tests_if_needed(
    node: DbtNode,
    task_args: dict[str, str],
    detached_from_parent: dict[str, DbtNode] | None = None,
) -> None:
    """
    Add exclude statements if there are tests associated to the model that should be run detached from the model/tests.

    Change task_args in-place.
    """
    if detached_from_parent is None:
        detached_from_parent = {}
    exclude: list[str] = task_args.get("exclude", [])  # type: ignore
    tests_detached_from_this_node: list[DbtNode] = detached_from_parent.get(node.unique_id, [])  # type: ignore
    for test_node in tests_detached_from_this_node:
        exclude.append(test_node.resource_name.split(".")[0])
    if exclude:
        task_args["exclude"] = exclude  # type: ignore


def _override_profile_if_needed(task_kwargs: dict[str, Any], profile_kwargs_override: dict[str, Any]) -> None:
    """
    Changes in-place the profile configuration if it needs to be overridden.
    """
    if profile_kwargs_override:
        modified_profile_config = deepcopy(task_kwargs["profile_config"])
        modified_profile_kwargs_override = deepcopy(profile_kwargs_override)
        profile_mapping_override = modified_profile_kwargs_override.pop("profile_mapping", {})
        for key, value in modified_profile_kwargs_override.items():
            setattr(modified_profile_config, key, value)
        if modified_profile_config.profile_mapping and profile_mapping_override:
            for key, value in profile_mapping_override.items():
                setattr(modified_profile_config.profile_mapping, key, value)
        task_kwargs["profile_config"] = modified_profile_config


def create_test_task_metadata(
    test_task_name: str,
    execution_mode: ExecutionMode,
    test_indirect_selection: TestIndirectSelection,
    task_args: dict[str, Any],
    on_warning_callback: Callable[..., Any] | None = None,
    node: DbtNode | None = None,
    render_config: RenderConfig | None = None,
    detached_from_parent: dict[str, DbtNode] | None = None,
) -> TaskMetadata:
    """
    Create the metadata that will be used to instantiate the Airflow Task that will be used to run the Dbt test node.

    :param test_task_name: Name of the Airflow task to be created
    :param execution_mode: The Cosmos execution mode we're aiming to run the dbt task at (e.g. local)
    :param task_args: Arguments to be used to instantiate an Airflow Task
    :param on_warning_callback: A callback function called on warnings with additional Context variables “test_names”
    and “test_results” of type List.
    :param node: If the test relates to a specific node, the node reference
    :param detached_from_parent: Dictionary that maps node ids and their children tests that should be run detached
    :returns: The metadata necessary to instantiate the source dbt node as an Airflow task.
    """
    task_args = dict(task_args)
    task_args["on_warning_callback"] = on_warning_callback
    extra_context = {}
    detached_from_parent = detached_from_parent or {}
    task_owner = ""

    if test_indirect_selection != TestIndirectSelection.EAGER:
        task_args["indirect_selection"] = test_indirect_selection.value
    if node is not None:
        if node.resource_type == DbtResourceType.MODEL:
            task_args["models"] = node.resource_name
        elif node.resource_type == DbtResourceType.SOURCE:
            task_args["select"] = f"source:{node.resource_name}"
        elif is_detached_test(node):
            task_args["select"] = node.resource_name.split(".")[0]
        else:  # tested with node.resource_type == DbtResourceType.SEED or DbtResourceType.SNAPSHOT
            task_args["select"] = node.resource_name

        extra_context = {"dbt_node_config": node.context_dict}
        task_owner = node.owner

    elif render_config is not None:  # TestBehavior.AFTER_ALL
        task_args["select"] = render_config.select
        task_args["selector"] = render_config.selector
        task_args["exclude"] = render_config.exclude

    if node:
        exclude_detached_tests_if_needed(node, task_args, detached_from_parent)
        _override_profile_if_needed(task_args, node.profile_config_to_override)

    args_to_override: dict[str, Any] = {}
    if node:
        args_to_override = node.operator_kwargs_to_override

    return TaskMetadata(
        id=test_task_name,
        owner=task_owner,
        operator_class=calculate_operator_class(
            execution_mode=execution_mode,
            dbt_class="DbtTest",
        ),
        arguments={**task_args, **args_to_override},
        extra_context=extra_context,
    )


def _get_task_id_and_args(
    node: DbtNode,
    args: dict[str, Any],
    use_task_group: bool,
    normalize_task_id: Callable[..., Any] | None,
    resource_suffix: str,
    include_resource_type: bool = False,
) -> tuple[str, dict[str, Any]]:
    """
    Generate task ID and update args with display name if needed.
    """
    args_update = args
    task_display_name = f"{node.name}_{resource_suffix}"
    if include_resource_type:
        task_display_name = f"{node.name}_{node.resource_type.value}_{resource_suffix}"
    if use_task_group:
        task_id = resource_suffix
    elif normalize_task_id:
        task_id = normalize_task_id(node)
        args_update["task_display_name"] = task_display_name
    else:
        task_id = task_display_name
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
    test_indirect_selection: TestIndirectSelection = TestIndirectSelection.EAGER,
    on_warning_callback: Callable[..., Any] | None = None,
    detached_from_parent: dict[str, DbtNode] | None = None,
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
    :param detached_from_parent: Dictionary that maps node ids and their children tests that should be run detached
    :returns: The metadata necessary to instantiate the source dbt node as an Airflow task.
    """
    dbt_resource_to_class = create_dbt_resource_to_class(test_behavior)

    args = {**args, **{"models": node.resource_name}}

    if DbtResourceType(node.resource_type) in DEFAULT_DBT_RESOURCES and node.resource_type in dbt_resource_to_class:
        extra_context: dict[str, Any] = {
            "dbt_node_config": node.context_dict,
            "dbt_dag_task_group_identifier": dbt_dag_task_group_identifier,
            "package_name": node.package_name,
        }

        if test_behavior == TestBehavior.BUILD and node.resource_type in SUPPORTED_BUILD_RESOURCES:
            if test_indirect_selection != TestIndirectSelection.EAGER:
                args["indirect_selection"] = test_indirect_selection.value
            args["on_warning_callback"] = on_warning_callback
            exclude_detached_tests_if_needed(node, args, detached_from_parent)
            task_id, args = _get_task_id_and_args(
                node, args, use_task_group, normalize_task_id, "build", include_resource_type=True
            )
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

        _override_profile_if_needed(args, node.profile_config_to_override)

        task_metadata = TaskMetadata(
            id=task_id,
            owner=node.owner,
            operator_class=calculate_operator_class(
                execution_mode=execution_mode, dbt_class=dbt_resource_to_class[node.resource_type]
            ),
            arguments={**args, **node.operator_kwargs_to_override},
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


def is_detached_test(node: DbtNode) -> bool:
    """
    Identify if node should be rendered detached from the parent. Conditions that should be met:
    * is a test
    * has multiple parents
    """
    if node.resource_type == DbtResourceType.TEST and len(node.depends_on) > 1:
        return True
    return False


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
    detached_from_parent: dict[str, DbtNode] | None = None,
    **kwargs: Any,
) -> BaseOperator | TaskGroup | None:
    task_or_group: BaseOperator | TaskGroup | None = None
    detached_from_parent = detached_from_parent or {}

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
        test_indirect_selection=test_indirect_selection,
        on_warning_callback=on_warning_callback,
        detached_from_parent=detached_from_parent,
    )

    # In most cases, we'll  map one DBT node to one Airflow task
    # The exception are the test nodes, since it would be too slow to run test tasks individually.
    # If test_behaviour=="after_each", each model task will be bundled with a test task, using TaskGroup
    if task_meta and not node.resource_type == DbtResourceType.TEST:
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
                    detached_from_parent=detached_from_parent,
                )
                test_task = create_airflow_task(test_meta, dag, task_group=model_task_group)
                task >> test_task
                task_or_group = model_task_group
        else:
            task_or_group = create_airflow_task(task_meta, dag, task_group=task_group)

    return task_or_group


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


def _add_dbt_setup_async_task(
    dag: DAG,
    execution_mode: ExecutionMode,
    task_args: dict[str, Any],
    tasks_map: dict[str, Any],
    task_group: TaskGroup | None,
    render_config: RenderConfig | None = None,
    async_py_requirements: list[str] | None = None,
) -> None:
    if execution_mode != ExecutionMode.AIRFLOW_ASYNC:
        return

    if not async_py_requirements:
        raise CosmosValueError("ExecutionConfig.AIRFLOW_ASYNC needs async_py_requirements to be set")

    if render_config is not None:
        task_args["select"] = render_config.select
        task_args["selector"] = render_config.selector
        task_args["exclude"] = render_config.exclude
        task_args["py_requirements"] = async_py_requirements

    setup_task_metadata = TaskMetadata(
        id=DBT_SETUP_ASYNC_TASK_ID,
        operator_class="cosmos.operators._asynchronous.SetupAsyncOperator",
        arguments=task_args,
        extra_context={"dbt_dag_task_group_identifier": _get_dbt_dag_task_group_identifier(dag, task_group)},
    )
    setup_airflow_task = create_airflow_task(setup_task_metadata, dag, task_group=task_group)

    for task_id, task in tasks_map.items():
        if not task.upstream_list:
            setup_airflow_task >> task

    tasks_map[DBT_SETUP_ASYNC_TASK_ID] = setup_airflow_task


def should_create_detached_nodes(render_config: RenderConfig) -> bool:
    """
    Decide if we should calculate / insert detached nodes into the graph.
    """
    return render_config.should_detach_multiple_parents_tests and render_config.test_behavior in (
        TestBehavior.BUILD,
        TestBehavior.AFTER_EACH,
    )


def identify_detached_nodes(
    nodes: dict[str, DbtNode],
    render_config: RenderConfig,
    detached_nodes: dict[str, DbtNode],
    detached_from_parent: dict[str, list[DbtNode]],
) -> None:
    """
    Given the nodes that represent a dbt project and the test_behavior, identify the detached test nodes
    (test nodes that have multiple dependencies and should run independently).

    Change in-place the dictionaries detached_nodes (detached node ID : node) and detached_from_parent (parent node ID that
    is upstream to this test and the test node).
    """
    if should_create_detached_nodes(render_config):
        for node_id, node in nodes.items():
            if is_detached_test(node):
                detached_nodes[node_id] = node
                for parent_id in node.depends_on:
                    detached_from_parent[parent_id].append(node)


_counter = 0


def calculate_detached_node_name(node: DbtNode) -> str:
    """
    Given a detached test node, calculate its name. It will either be:
     - the name of the test with a "_test" suffix, if this is smaller than 250
     - or detached_{an incremental number}_test
    """
    # Note: this implementation currently relies on the fact that Airflow creates a new process
    # to parse each DAG both in the scheduler and also in the worker nodes. We logged a ticket to improved this:
    # https://github.com/astronomer/astronomer-cosmos/issues/1469
    node_name = f"{node.resource_name.split('.')[0]}_test"
    if not len(node_name) < AIRFLOW_MAX_ID_LENGTH:
        global _counter
        node_name = f"detached_{_counter}_test"
        _counter += 1
    return node_name


def _add_teardown_task(
    dag: DAG,
    execution_mode: ExecutionMode,
    task_args: dict[str, Any],
    tasks_map: dict[str, Any],
    task_group: TaskGroup | None,
    render_config: RenderConfig | None = None,
    async_py_requirements: list[str] | None = None,
) -> None:
    if execution_mode != ExecutionMode.AIRFLOW_ASYNC:
        return

    if not async_py_requirements:
        raise CosmosValueError("ExecutionConfig.AIRFLOW_ASYNC needs async_py_requirements to be set")

    if render_config is not None:
        task_args["select"] = render_config.select
        task_args["selector"] = render_config.selector
        task_args["exclude"] = render_config.exclude
        task_args["py_requirements"] = async_py_requirements

    teardown_task_metadata = TaskMetadata(
        id=DBT_TEARDOWN_ASYNC_TASK_ID,
        operator_class="cosmos.operators._asynchronous.TeardownAsyncOperator",
        arguments=task_args,
        extra_context={"dbt_dag_task_group_identifier": _get_dbt_dag_task_group_identifier(dag, task_group)},
    )
    teardown_airflow_task = create_airflow_task(teardown_task_metadata, dag, task_group=task_group)

    for task_id, task in tasks_map.items():
        if len(task.downstream_list) == 0:
            task >> teardown_airflow_task

    tasks_map[DBT_TEARDOWN_ASYNC_TASK_ID] = teardown_airflow_task


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
    async_py_requirements: list[str] | None = None,
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

    # Identify test nodes that should be run detached from the associated dbt resource nodes because they
    # have multiple parents
    detached_nodes: dict[str, DbtNode] = OrderedDict()
    detached_from_parent: dict[str, list[DbtNode]] = defaultdict(list)
    identify_detached_nodes(nodes, render_config, detached_nodes, detached_from_parent)

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
            detached_from_parent=detached_from_parent,
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
    elif test_behavior in (TestBehavior.BUILD, TestBehavior.AFTER_EACH):
        # Handle detached test nodes
        for node_id, node in detached_nodes.items():
            datached_node_name = calculate_detached_node_name(node)
            test_meta = create_test_task_metadata(
                datached_node_name,
                execution_mode,
                test_indirect_selection,
                task_args=task_args,
                on_warning_callback=on_warning_callback,
                render_config=render_config,
                node=node,
            )
            test_task = create_airflow_task(test_meta, dag, task_group=task_group)
            tasks_map[node_id] = test_task

    create_airflow_task_dependencies(nodes, tasks_map)
    if enable_setup_async_task:
        _add_dbt_setup_async_task(
            dag,
            execution_mode,
            task_args,
            tasks_map,
            task_group,
            render_config=render_config,
            async_py_requirements=async_py_requirements,
        )
    if enable_teardown_async_task:
        _add_teardown_task(
            dag,
            execution_mode,
            task_args,
            tasks_map,
            task_group,
            render_config=render_config,
            async_py_requirements=async_py_requirements,
        )
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
