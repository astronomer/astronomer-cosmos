from __future__ import annotations

from collections import OrderedDict, defaultdict
from collections.abc import Callable
from copy import deepcopy
from typing import Any

try:  # Airflow 3
    from airflow.sdk.bases.operator import BaseOperator
except ImportError:  # Airflow 2
    from airflow.models import BaseOperator

from airflow.models.base import ID_LEN as AIRFLOW_MAX_ID_LENGTH
from airflow.models.dag import DAG

try:
    # Airflow 3.1 onwards
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup

from cosmos import settings
from cosmos.config import ExecutionConfig, RenderConfig
from cosmos.constants import (
    DBT_SETUP_ASYNC_TASK_ID,
    DBT_TEARDOWN_ASYNC_TASK_ID,
    DEFAULT_DBT_RESOURCES,
    PRODUCER_WATCHER_TASK_ID,
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

logger = get_logger(__name__)


def _convert_list_to_str(value: list[str] | str | None) -> str | None:
    """Convert a list of strings to a space-separated string for dbt CLI flags.

    RenderConfig stores select/exclude as list[str], but AbstractDbtBase operators
    expect them as str | None. This helper bridges the two interfaces without
    requiring a breaking change to AbstractDbtBase.
    """
    if isinstance(value, list):
        return " ".join(value) if value else None
    return value


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


def _is_source_used_by_filtered_nodes(source_node: DbtNode, filtered_nodes: dict[str, DbtNode]) -> bool:
    """
    Check if a source node is referenced by any of the filtered nodes.

    :param source_node: The source node to check
    :param filtered_nodes: Dictionary of filtered nodes
    :returns: True if the source is used by any filtered node, False otherwise
    """
    source_id = source_node.unique_id

    # Check if any filtered node depends on this source
    for node in filtered_nodes.values():
        if source_id in node.depends_on:
            return True

    return False


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
    detached_from_parent: dict[str, list[DbtNode]] | None = None,
) -> None:
    """
    Add exclude statements if there are tests associated to the model that should be run detached from the model/tests.

    Change task_args in-place.
    """
    if detached_from_parent is None:
        detached_from_parent = {}
    current_exclude = task_args.get("exclude")
    # Handle both list[str] (legacy) and str formats for backward compatibility
    if isinstance(current_exclude, list):
        exclude_items = current_exclude
    elif isinstance(current_exclude, str):
        exclude_items = current_exclude.split() if current_exclude else []
    else:
        exclude_items = []
    tests_detached_from_this_node: list[DbtNode] = detached_from_parent.get(node.unique_id, [])  # type: ignore
    for test_node in tests_detached_from_this_node:
        exclude_items.append(test_node.resource_name.split(".")[0])
    if exclude_items:
        task_args["exclude"] = _convert_list_to_str(exclude_items) or ""


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
    detached_from_parent: dict[str, list[DbtNode]] | None = None,
    enable_owner_inheritance: bool | None = None,
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
        if node.resource_type == DbtResourceType.SOURCE:
            task_args["select"] = f"source:{node.resource_name}"
        elif is_detached_test(node):
            task_args["select"] = node.resource_name.split(".")[0]
        else:  # tested with node.resource_type == DbtResourceType.SEED or DbtResourceType.SNAPSHOT
            task_args["select"] = node.resource_name

        extra_context = {"dbt_node_config": node.context_dict}
        task_owner = node.owner
    elif render_config is not None:  # TestBehavior.AFTER_ALL
        task_args["select"] = _convert_list_to_str(render_config.select)
        task_args["selector"] = render_config.selector
        task_args["exclude"] = _convert_list_to_str(render_config.exclude)

    if node:
        exclude_detached_tests_if_needed(node, task_args, detached_from_parent)
        _override_profile_if_needed(task_args, node.profile_config_to_override)

    if not enable_owner_inheritance:
        task_owner = ""

    args_to_override: dict[str, Any] = {}
    if node:
        args_to_override = node.operator_kwargs_to_override

    dbt_class = "DbtTest"
    watcher_to_test_execution_mode = {
        ExecutionMode.WATCHER: ExecutionMode.LOCAL,
        ExecutionMode.WATCHER_KUBERNETES: ExecutionMode.KUBERNETES,
    }
    if (
        render_config is not None
        and render_config.test_behavior == TestBehavior.AFTER_ALL
        and execution_mode in (ExecutionMode.WATCHER, ExecutionMode.WATCHER_KUBERNETES)
    ):
        test_execution_mode = watcher_to_test_execution_mode[execution_mode]
        operator_class = calculate_operator_class(execution_mode=test_execution_mode, dbt_class=dbt_class)
    else:
        operator_class = calculate_operator_class(execution_mode=execution_mode, dbt_class=dbt_class)

    return TaskMetadata(
        id=test_task_name,
        owner=task_owner,
        operator_class=operator_class,
        arguments={**task_args, **args_to_override},
        extra_context=extra_context,
    )


def _get_task_id_and_args(
    node: DbtNode,
    args: dict[str, Any],
    use_task_group: bool,
    execution_mode: ExecutionMode,
    normalize_task_id: Callable[..., Any] | None,
    normalize_task_display_name: Callable[..., Any] | None,
    resource_suffix: str,
    include_resource_type: bool = False,
) -> tuple[str, dict[str, Any]]:
    """
    Generate task ID and update args with display name if needed.
    """
    args_update = args
    task_name = f"{node.name}_{resource_suffix}"

    if include_resource_type:
        task_name = f"{node.name}_{node.resource_type.value}_{resource_suffix}"

    if use_task_group:
        task_id = resource_suffix

    elif normalize_task_id and not normalize_task_display_name:
        task_id = normalize_task_id(node)
        args_update["task_display_name"] = task_name
    elif not normalize_task_id and normalize_task_display_name:
        task_id = task_name
        args_update["task_display_name"] = normalize_task_display_name(node)
    elif normalize_task_id and normalize_task_display_name:
        task_id = normalize_task_id(node)
        args_update["task_display_name"] = normalize_task_display_name(node)
    else:
        task_id = task_name

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
    render_config: RenderConfig = RenderConfig(),
    use_task_group: bool = False,
    test_indirect_selection: TestIndirectSelection = TestIndirectSelection.EAGER,
    on_warning_callback: Callable[..., Any] | None = None,
    detached_from_parent: dict[str, list[DbtNode]] | None = None,
    filtered_nodes: dict[str, DbtNode] | None = None,
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
    dbt_resource_to_class = create_dbt_resource_to_class(render_config.test_behavior)

    # Make a copy to avoid issues with mutable arguments
    args = {**args}

    if DbtResourceType(node.resource_type) in DEFAULT_DBT_RESOURCES and node.resource_type in dbt_resource_to_class:
        extra_context: dict[str, Any] = {
            "dbt_node_config": node.context_dict,
            "dbt_dag_task_group_identifier": dbt_dag_task_group_identifier,
            "package_name": node.package_name,
        }
        resource_suffix_map = {TestBehavior.BUILD: "build", DbtResourceType.MODEL: "run"}
        resource_suffix = (
            resource_suffix_map.get(render_config.test_behavior)
            or resource_suffix_map.get(node.resource_type)
            or node.resource_type.value
        )
        # Since Cosmos 1.11, it selects models using --select, instead of --models. The reason for this is that
        # this flag was deprecated in dbt-core 1.10 (https://github.com/dbt-labs/dbt-core/issues/11561)
        # and dbt fusion (2.0.0-beta26) does not support it.
        # Users can still force Cosmos to use `--models` by setting the environment variable
        # `AIRFLOW__COSMOS__PRE_DBT_FUSION=1`.
        models_select_key = "models" if settings.pre_dbt_fusion else "select"

        if render_config.test_behavior == TestBehavior.BUILD and node.resource_type in SUPPORTED_BUILD_RESOURCES:
            args[models_select_key] = f"{node.resource_name}"
            if test_indirect_selection != TestIndirectSelection.EAGER:
                args["indirect_selection"] = test_indirect_selection.value
            args["on_warning_callback"] = on_warning_callback
            exclude_detached_tests_if_needed(node, args, detached_from_parent)
            task_id, args = _get_task_id_and_args(
                node=node,
                args=args,
                use_task_group=use_task_group,
                normalize_task_id=render_config.normalize_task_id,
                normalize_task_display_name=render_config.normalize_task_display_name,
                resource_suffix=resource_suffix,
                include_resource_type=True,
                execution_mode=execution_mode,
            )
        elif node.resource_type == DbtResourceType.SOURCE:
            args["select"] = f"source:{node.resource_name}"
            args["on_warning_callback"] = on_warning_callback

            if (render_config.source_rendering_behavior == SourceRenderingBehavior.NONE) or (
                render_config.source_rendering_behavior == SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS
                and node.has_freshness is False
                and node.has_test is False
            ):
                return None

            if (
                render_config.source_pruning
                and filtered_nodes
                and not _is_source_used_by_filtered_nodes(node, filtered_nodes)
            ):
                return None
            task_id, args = _get_task_id_and_args(
                node=node,
                args=args,
                use_task_group=use_task_group,
                normalize_task_id=render_config.normalize_task_id,
                normalize_task_display_name=render_config.normalize_task_display_name,
                resource_suffix=r"source",
                execution_mode=execution_mode,
            )
            if node.has_freshness is False and render_config.source_rendering_behavior == SourceRenderingBehavior.ALL:
                # render sources without freshness as empty operators
                # empty operator does not accept custom parameters (e.g., profile_args). recreate the args.
                if "task_display_name" in args:
                    args = {"task_display_name": args["task_display_name"]}
                else:
                    args = {}
                return TaskMetadata(id=task_id, operator_class="airflow.operators.empty.EmptyOperator", arguments=args)
        else:  # DbtResourceType.MODEL, DbtResourceType.SEED and DbtResourceType.SNAPSHOT
            args[models_select_key] = node.resource_name
            task_id, args = _get_task_id_and_args(
                node=node,
                args=args,
                use_task_group=use_task_group,
                normalize_task_id=render_config.normalize_task_id,
                normalize_task_display_name=render_config.normalize_task_display_name,
                resource_suffix=resource_suffix,
                execution_mode=execution_mode,
            )

        _override_profile_if_needed(args, node.profile_config_to_override)

        task_owner = node.owner

        if not render_config.enable_owner_inheritance:
            task_owner = ""

        task_metadata = TaskMetadata(
            id=task_id,
            owner=task_owner,
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


def generate_or_convert_task(
    task_meta: TaskMetadata,
    dag: DAG,
    task_group: TaskGroup | None,
    node: DbtNode,
    resource_type: DbtResourceType,
    task_args: dict[str, Any],
    render_config: RenderConfig,
    node_converters: dict[DbtResourceType, Callable[..., Any]],
    on_warning_callback: Callable[..., Any] | None,
    # Properties from ExecutionConfig:
    execution_mode: ExecutionMode,
    test_indirect_selection: TestIndirectSelection,
    # Other arguments relevant to instantiating the task:
    dbt_project_name: str | None = None,
    detached_from_parent: dict[str, list[DbtNode]] | None = None,
    **kwargs: Any,
) -> BaseOperator:
    """
    Checks if a node_converter was supplied for the given resource type:
      - If yes, attempts to convert the task using the given node_converter
      - If no, creates the task as expected with the supplied task_meta
    Returns the created task
    """
    task: BaseOperator

    conversion_function = node_converters.get(resource_type, None)
    if conversion_function is not None:
        task_id = task_meta.id
        logger.debug(f"Converting node <{node.unique_id}> task <{task_id}> using <{conversion_function.__name__}>")
        # In Cosmos 2.0 we should review this implementation and use render_config or another simpler interface:
        task = conversion_function(  # type: ignore
            dag=dag,
            task_group=task_group,
            dbt_project_name=dbt_project_name,
            execution_mode=execution_mode,
            task_args=task_args,
            test_behavior=render_config.test_behavior,
            source_pruning=render_config.source_pruning,
            source_rendering_behavior=render_config.source_rendering_behavior,
            normalize_task_id=render_config.normalize_task_id,
            normalize_task_display_name=render_config.normalize_task_display_name,
            enable_owner_inheritance=render_config.enable_owner_inheritance,
            test_indirect_selection=test_indirect_selection,
            on_warning_callback=on_warning_callback,
            node=node,
            task_id=task_id,
            detached_from_parent=detached_from_parent,
        )
        if task is not None:
            logger.debug(f"Conversion of node <{node.unique_id}> task <{task_id}> was successful!")
    else:
        task = create_airflow_task(task_meta, dag, task_group)
    return task


def generate_task_or_group(
    dag: DAG,
    task_group: TaskGroup | None,
    node: DbtNode,
    task_args: dict[str, Any],
    render_config: RenderConfig,
    node_converters: dict[DbtResourceType, Callable[..., Any]],
    # These two properties come from ExecutionConfig and affect rendering decisions:
    execution_mode: ExecutionMode,
    test_indirect_selection: TestIndirectSelection,
    # Other arguments relevant to instantiating that task or task group:
    dbt_project_name: str | None = None,
    on_warning_callback: Callable[..., Any] | None = None,
    detached_from_parent: dict[str, list[DbtNode]] | None = None,
    filtered_nodes: dict[str, DbtNode] | None = None,
    **kwargs: Any,
) -> BaseOperator | TaskGroup | None:

    task_or_group: BaseOperator | TaskGroup | None = None
    detached_from_parent = detached_from_parent or {}
    use_task_group = (
        node.resource_type in TESTABLE_DBT_RESOURCES
        and render_config.test_behavior == TestBehavior.AFTER_EACH
        and node.has_non_detached_test is True
    )
    convert_entire_task_group = render_config.node_conversion_by_task_group and node.resource_type in node_converters

    task_meta = create_task_metadata(
        node=node,
        render_config=render_config,
        execution_mode=execution_mode,
        args=task_args,
        dbt_dag_task_group_identifier=_get_dbt_dag_task_group_identifier(dag, task_group),
        use_task_group=use_task_group,
        test_indirect_selection=test_indirect_selection,
        on_warning_callback=on_warning_callback,
        detached_from_parent=detached_from_parent,
        filtered_nodes=filtered_nodes,
    )

    generate_or_convert_task_args = {
        "task_meta": task_meta,
        "dbt_project_name": dbt_project_name,
        "dag": dag,
        "task_group": task_group,
        "node": node,
        "resource_type": node.resource_type,
        "task_args": task_args,
        "render_config": render_config,
        "on_warning_callback": on_warning_callback,
        "detached_from_parent": detached_from_parent,
        "node_converters": node_converters,
        # Properties from ExecutionConfig:
        "execution_mode": execution_mode,
        "test_indirect_selection": test_indirect_selection,
    }

    # In most cases, we'll  map one DBT node to one Airflow task
    # The exception are the test nodes, since it would be too slow to run test tasks individually.
    # If test_behaviour=="after_each", each model task will be bundled with a test task, using TaskGroup
    if task_meta and not node.resource_type == DbtResourceType.TEST:
        if use_task_group and (not convert_entire_task_group):
            with TaskGroup(dag=dag, group_id=node.name, parent_group=task_group) as model_task_group:
                task = generate_or_convert_task(
                    **{  # type: ignore[arg-type]
                        **generate_or_convert_task_args,
                        "task_group": model_task_group,
                    },
                )
                test_meta = create_test_task_metadata(
                    "test",
                    execution_mode,
                    test_indirect_selection,
                    task_args=task_args,
                    node=node,
                    on_warning_callback=on_warning_callback,
                    detached_from_parent=detached_from_parent,
                    enable_owner_inheritance=render_config.enable_owner_inheritance,
                )
                test_task_generate_or_convert_task_args = {
                    **generate_or_convert_task_args,
                    "task_group": model_task_group,
                    "task_meta": test_meta,
                    "resource_type": DbtResourceType.TEST,  # type: ignore
                }
                test_task = generate_or_convert_task(**test_task_generate_or_convert_task_args)  # type: ignore[arg-type]
                task >> test_task
                task_or_group = model_task_group
        else:
            task_or_group = generate_or_convert_task(**generate_or_convert_task_args)  # type: ignore[arg-type]

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
        task_args["select"] = _convert_list_to_str(render_config.select)
        task_args["selector"] = render_config.selector
        task_args["exclude"] = _convert_list_to_str(render_config.exclude)
        task_args["py_requirements"] = async_py_requirements

    setup_task_metadata = TaskMetadata(
        id=DBT_SETUP_ASYNC_TASK_ID,
        operator_class="cosmos.operators._asynchronous.SetupAsyncOperator",
        arguments=task_args,
        extra_context={"dbt_dag_task_group_identifier": _get_dbt_dag_task_group_identifier(dag, task_group)},
    )
    setup_airflow_task = create_airflow_task(setup_task_metadata, dag, task_group=task_group)

    for node_id, task_or_taskgroup in tasks_map.items():
        node_tasks = (
            list(task_or_taskgroup.children.values())
            if isinstance(task_or_taskgroup, TaskGroup)
            else [task_or_taskgroup]
        )
        for task in node_tasks:
            task.producer_task_id = setup_airflow_task.task_id  # type: ignore[attr-defined]
            if not task.upstream_list:
                setup_airflow_task >> task

    tasks_map[DBT_SETUP_ASYNC_TASK_ID] = setup_airflow_task


def _add_watcher_producer_task(
    dag: DAG,
    task_args: dict[str, Any],
    tasks_map: dict[str, Any],
    task_group: TaskGroup | None,
    render_config: RenderConfig | None = None,
    execution_mode: ExecutionMode = ExecutionMode.WATCHER,
) -> BaseOperator:
    """
    Create the producer task for the watcher execution mode and add it to the tasks_map.
    The producer task is the task that will be used to produce the events for the watcher execution mode.
    """
    producer_task_args = task_args.copy()

    if render_config is not None:
        producer_task_args["select"] = _convert_list_to_str(render_config.select)
        producer_task_args["selector"] = render_config.selector
        producer_task_args["exclude"] = _convert_list_to_str(render_config.exclude)

        if render_config.test_behavior in [TestBehavior.NONE, TestBehavior.AFTER_ALL]:
            additional_excludes = "resource_type:test resource_type:unit_test"
            current_exclude = producer_task_args.get("exclude")
            if current_exclude:
                producer_task_args["exclude"] = f"{current_exclude} {additional_excludes}"
            else:
                producer_task_args["exclude"] = additional_excludes

    class_name = calculate_operator_class(execution_mode, "DbtProducer")

    # First, we create the producer task
    producer_task_metadata = TaskMetadata(
        id=PRODUCER_WATCHER_TASK_ID,
        operator_class=class_name,
        arguments=producer_task_args,
    )
    producer_airflow_task = create_airflow_task(producer_task_metadata, dag, task_group=task_group)
    tasks_map[PRODUCER_WATCHER_TASK_ID] = producer_airflow_task
    return producer_airflow_task


def _add_watcher_dependencies(
    dag: DAG,
    producer_airflow_task: BaseOperator,
    task_args: dict[str, Any],
    tasks_map: dict[str, Any],
    nodes: dict[str, DbtNode] | None = None,
) -> None:
    """
    Iterate through the watcher consumer tasks and:
    - set the producer task ID in all of them
    - make the producer task to be the parent of the root dbt nodes, without blocking them from sensing XCom
    """
    for node_id, task_or_taskgroup in tasks_map.items():
        # We do not want to set a dependency between the producer task and itself
        if node_id == PRODUCER_WATCHER_TASK_ID:
            continue

        node_tasks = (
            list(task_or_taskgroup.children.values())
            if isinstance(task_or_taskgroup, TaskGroup)
            else [task_or_taskgroup]
        )
        for task in node_tasks:
            task.producer_task_id = producer_airflow_task.task_id  # type: ignore[attr-defined]

        # Make the producer task to be the parent of the root dbt nodes, without blocking them from sensing XCom
        # We only managed to do this in the case of DbtDag.
        # The way it is implemented is by setting the trigger_rule to "always" for the consumer tasks, and by having the producer task with a high priority_weight.
        if "DbtDag" in dag.__class__.__name__:
            # Is this dbt node a root of the (subset of the) dbt project?
            # Note: this may happen in one scenarios:
            # - the dbt node not having any `depends_on` within the user-selected `nodes`
            if nodes and node_id in nodes and not set(nodes[node_id].depends_on).intersection(nodes):
                producer_airflow_task >> task_or_taskgroup
                if isinstance(task_or_taskgroup, TaskGroup):
                    taskgroup = task_or_taskgroup
                    always_run_tasks = [
                        task for task in node_tasks if not set(task.upstream_task_ids).intersection(taskgroup.children)
                    ]
                else:
                    always_run_tasks = [task_or_taskgroup]
                for task in always_run_tasks:
                    task.trigger_rule = task_args.get("trigger_rule", "always")  # type: ignore[attr-defined]


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
        task_args["select"] = _convert_list_to_str(render_config.select)
        task_args["selector"] = render_config.selector
        task_args["exclude"] = _convert_list_to_str(render_config.exclude)
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


def build_airflow_graph(  # noqa: C901 TODO: https://github.com/astronomer/astronomer-cosmos/issues/1943
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
    execution_config: ExecutionConfig | None = None,
) -> dict[str, TaskGroup | BaseOperator]:
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
    tasks_map: dict[str, TaskGroup | BaseOperator] = {}
    task_or_group: TaskGroup | BaseOperator | None
    producer_task: BaseOperator | None = None

    # Identify test nodes that should be run detached from the associated dbt resource nodes because they
    # have multiple parents
    detached_nodes: dict[str, DbtNode] = OrderedDict()
    detached_from_parent: dict[str, list[DbtNode]] = defaultdict(list)
    identify_detached_nodes(nodes, render_config, detached_nodes, detached_from_parent)

    virtualenv_dir = None

    if execution_mode == ExecutionMode.AIRFLOW_ASYNC:
        # This property is only relevant for the setup task, not the other tasks:
        virtualenv_dir = task_args.pop("virtualenv_dir", None)
    elif execution_mode in (ExecutionMode.WATCHER, ExecutionMode.WATCHER_KUBERNETES):
        setup_operator_args = getattr(execution_config, "setup_operator_args", None) or {}
        # We are intentionally creating the producer task ahead of the consumer tasks
        # Airflow priority weight is not being respected in multiple versions of the library, including 3.1
        # To instantiate the producer before helps having it before on the DAG topological order and scheduling this task before the consumer tasks
        producer_task = _add_watcher_producer_task(
            dag=dag,
            task_args={**task_args, **setup_operator_args},
            tasks_map=tasks_map,
            task_group=task_group,
            render_config=render_config,
            execution_mode=execution_mode,
        )

    for node_id, node in nodes.items():
        task_or_group_args = {
            # Arguments to this method:
            "dag": dag,
            "task_group": task_group,
            "node": node,
            "task_args": task_args,
            "dbt_project_name": dbt_project_name,
            "render_config": render_config,
            # Properties from ExecutionConfig:
            "execution_mode": execution_mode,
            "test_indirect_selection": test_indirect_selection,
            # Argument to DbtDag or DbtTaskGroup:
            "on_warning_callback": on_warning_callback,
            # Calculated in this method:
            "detached_from_parent": detached_from_parent,
            "node_converters": render_config.node_converters or {},
        }

        task_or_group = generate_task_or_group(**task_or_group_args, filtered_nodes=nodes)  # type: ignore[arg-type]
        if task_or_group is not None:
            tasks_map[node_id] = task_or_group

    # If test_behaviour=="after_all", there will be one test task, run by the end of the DAG
    # The end of a DAG is defined by the DAG leaf tasks (tasks which do not have downstream tasks)
    if render_config.test_behavior == TestBehavior.AFTER_ALL:
        test_meta = create_test_task_metadata(
            f"{dbt_project_name}_test",
            execution_mode,
            test_indirect_selection,
            task_args=task_args,
            on_warning_callback=on_warning_callback,
            render_config=render_config,
            enable_owner_inheritance=render_config.enable_owner_inheritance,
        )
        test_task_args = {
            **task_or_group_args,
            "task_meta": test_meta,
            "resource_type": DbtResourceType.TEST,  # type: ignore
        }
        test_task = generate_or_convert_task(**test_task_args)  # type: ignore[arg-type]
        leaves_ids = calculate_leaves(tasks_ids=list(tasks_map.keys()), nodes=nodes)
        for leaf_node_id in leaves_ids:
            tasks_map[leaf_node_id] >> test_task
    elif render_config.test_behavior in (TestBehavior.BUILD, TestBehavior.AFTER_EACH):
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
                enable_owner_inheritance=render_config.enable_owner_inheritance,
            )
            test_task_args = {
                **task_or_group_args,
                "task_meta": test_meta,
                "resource_type": node.resource_type,  # type: ignore
            }
            test_task = generate_or_convert_task(**test_task_args)  # type: ignore[arg-type]
            tasks_map[node_id] = test_task

    create_airflow_task_dependencies(nodes, tasks_map)

    if producer_task:
        _add_watcher_dependencies(
            dag=dag,
            producer_airflow_task=producer_task,
            task_args=task_args,
            tasks_map=tasks_map,
            nodes=nodes,
        )

    if settings.enable_setup_async_task:
        _add_dbt_setup_async_task(
            dag,
            execution_mode,
            {**task_args, "virtualenv_dir": virtualenv_dir},
            tasks_map,
            task_group=task_group,
            render_config=render_config,
            async_py_requirements=async_py_requirements,
        )
    if settings.enable_teardown_async_task:
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
    tasks_map: dict[str, TaskGroup | BaseOperator],
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
