from __future__ import annotations
import itertools
import json
import logging
from dataclasses import dataclass
from subprocess import Popen, PIPE
from typing import Any, Callable

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos.core.airflow import get_airflow_task as create_airflow_task
from cosmos.core.graph.entities import Task as TaskMetadata
from cosmos.dataset import get_dbt_dataset
from cosmos.dbt.parser.project import DbtProject
from cosmos.render import calculate_operator_class


logger = logging.getLogger(__name__)

# TODO replace inline constants


@dataclass
class DbtNode:
    name: str
    unique_id: str
    resource_type: str
    depends_on: list[str]
    file_path: str
    tags: list[str]
    config: dict[str, Any]


# TODO: implement this:
def extract_dbt_nodes_from_manifest():
    pass


def extract_dbt_nodes(
    project_dir,
    exclude=None,
    select=None,
    # models=None,
    # selector=None,
    # resource_type=None,
    # profile_name="dbt_project.yml",
):
    # TODO: have a reliable way of checking if dbt is available

    # TODO: May need to create the profile in advance
    # Have an alternative for K8s & Docker executor
    command = ["dbt", "ls", "--output", "json", "--profiles-dir", project_dir]
    if exclude is not None:
        command.extend(["--exclude", ",".join(exclude)])
    if select is not None:
        command.extend(["--select", ",".join(select)])

    process = Popen(command, stdout=PIPE, stderr=PIPE, cwd=project_dir)
    stdout, stderr = process.communicate()

    nodes = {}
    for line in stdout.decode().split("\n"):
        try:
            node_dict = json.loads(line.strip())
        except json.decoder.JSONDecodeError:
            logger.info("Skipping line: %s", line)
        else:
            node = DbtNode(
                name=node_dict["name"],
                unique_id=node_dict["unique_id"],
                resource_type=node_dict["resource_type"],
                depends_on=node_dict["depends_on"].get("nodes", []),
                file_path=project_dir / node_dict["original_file_path"],
                tags=node_dict["tags"],
                config=node_dict["config"],
            )
            nodes[node.unique_id] = node

    return nodes


def convert_legacy_project_to_nodes(dbt_root_path, dbt_models_dir, dbt_snapshots_dir, dbt_seeds_dir, dbt_project_name):
    """
    Convert from the original project to the new list of nodes representation
    """
    project = DbtProject(
        dbt_root_path=dbt_root_path,
        dbt_models_dir=dbt_models_dir,
        dbt_snapshots_dir=dbt_snapshots_dir,
        dbt_seeds_dir=dbt_seeds_dir,
        project_name=dbt_project_name,
    )
    nodes = {}
    for model_name, model in itertools.chain(project.models.items(), project.snapshots.items(), project.seeds.items()):
        config = {item.split(":")[0]: item.split(":")[-1] for item in model.config.config_selectors}
        node = DbtNode(
            name=model_name,
            unique_id=model_name,
            resource_type=model.type,
            depends_on=model.config.upstream_models,
            file_path=model.path,
            tags=[],
            config=config,  # already contains tags
        )
        nodes[model_name] = node
    return nodes


def validate_arguments(select, exclude, profile_args, task_args):
    # ensures the same tag isn't in select & exclude
    if "tags" in select and "tags" in exclude:
        if set(select["tags"]).intersection(exclude["tags"]):
            raise AirflowException(
                f"Can't specify the same tag in `select` and `include`: "
                f"{set(select['tags']).intersection(exclude['tags'])}"
            )

    if "paths" in select and "paths" in exclude:
        if set(select["paths"]).intersection(exclude["paths"]):
            raise AirflowException(
                f"Can't specify the same path in `select` and `include`: "
                f"{set(select['paths']).intersection(exclude['paths'])}"
            )

    # if task_args has a schema, add it to the profile args and add a deprecated warning
    if "schema" in task_args:
        profile_args["schema"] = task_args["schema"]
        logger.warning("Specifying a schema in the task_args is deprecated. Please use the profile_args instead.")


def filter_nodes(project_dir, nodes, select, exclude):
    chosen_nodes = nodes.copy()

    def build_paths(selected_paths):
        return [(project_dir / path.strip("/")) for path in selected_paths]

    SUPPORTED_CONFIG = ["materialized", "schema", "tags"]

    def normalize_config(config):
        values = []
        for key, value in config.items():
            if key in SUPPORTED_CONFIG:
                if isinstance(value, list):
                    normalized = [f"{key}:{item}" for item in value]
                    values.extend(normalized)
                else:
                    values.append(f"{key}:{value}")
        return values

    for node_id, node in nodes.items():
        # path-based filters
        paths_to_include = build_paths(select.get("paths", []))
        paths_to_exclude = build_paths(exclude.get("paths", []))
        node_parent_paths = node.file_path.parents()
        if not set(paths_to_include).intersection(node_parent_paths) or set(paths_to_exclude).intersection(
            node_parent_paths
        ):
            chosen_nodes.pop(node_id)

        # config-based filters
        configs_to_include = normalize_config(select.get("configs", []))
        configs_to_exclude = normalize_config(exclude.get("configs", []))
        # TODO: add tags
        node_config = normalize_config(node.config)
        if not set(configs_to_include).intersection(node_config) or set(configs_to_exclude).intersection(node_config):
            chosen_nodes.pop(node_id)

        return chosen_nodes


def calculate_leaves(nodes):
    """
    Tasks which are not parents (dependencies) to other tasks.
    """
    parents = []
    leaves = []
    [parents.extend(node.depends_on) for node in nodes]
    parents_ids = set(parents)
    for node in nodes:
        if node.unique_id not in parents_ids:
            leaves.append(node)
    return leaves


def create_task_metadata(node: DbtNode, execution_mode, args):
    dbt_resource_to_class = {"model": "DbtRun", "snapshot": "DbtSnapshot", "seed": "DbtSeed", "test": "DbtTest"}
    task_id_suffix = "run" if node.resource_type == "model" else node.resource_type
    if node.resource_type in dbt_resource_to_class:
        task_metadata = TaskMetadata(
            id=f"{node.name}_{task_id_suffix}",
            operator_class=calculate_operator_class(
                execution_mode=execution_mode, dbt_class=dbt_resource_to_class[node.resource_type]
            ),
            arguments=args,
        )
        return task_metadata
    else:
        # Example of task that is currently skipped: "test"
        logger.error(f"Unsupported resource type {node.resource_type} (node {node.unique_id}).")


def create_test_task_metadata(test_task_name, execution_mode, task_args, on_warning_callback, model_name=None):
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


def add_airflow_entities(
    nodes: list[DbtNode],
    dag: DAG,  # Airflow-specific - parent DAG where to associate tasks and (optional) task groups
    execution_mode: str,  # Cosmos-specific - decide what which class to use
    task_args,  # Cosmos/DBT - used to instantiate tasks
    test_behavior,  # Cosmos-specific: how to inject tests to Airflow DAG
    dbt_project_name,  # DBT / Cosmos - used to name test task if mode is after_all,
    conn_id,  # Cosmos, dataset URI
    task_group: TaskGroup | None = None,
    on_warning_callback: Callable | None = None,  # argument specific to the DBT test command
    emit_datasets: bool = True,  # Cosmos
):
    tasks_map = {}

    # In most cases, we'll  map one DBT node to one Airflow task
    # The exception are the test nodes, since it would be too slow to run test tasks individually.
    # If test_behaviour=="after_each", each model task will be bundled with a test task, using TaskGroup
    for node_id, node in nodes.items():
        task_meta = create_task_metadata(node=node, execution_mode=execution_mode, args=task_args)
        if emit_datasets:
            task_args["outlets"] = [get_dbt_dataset(conn_id, dbt_project_name, node.name)]
        if task_meta and node.resource_type != "test":
            if node.resource_type == "model" and test_behavior == "after_each":
                with TaskGroup(dag=dag, group_id=node.name, parent_group=task_group) as model_task_group:
                    task = create_airflow_task(task_meta, dag, task_group=model_task_group)
                    test_meta = create_test_task_metadata(
                        f"{node.name}_test",
                        execution_mode,
                        task_args=task_args,
                        model_name=node.unique_id,
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
    if test_behavior == "after_all":
        task_args.pop("outlets", None)
        test_meta = create_test_task_metadata(
            f"{dbt_project_name}_test", execution_mode, task_args=task_args, on_warning_callback=on_warning_callback
        )
        test_task = create_airflow_task(test_meta, dag, task_group=task_group)
        leaves = calculate_leaves(nodes)
        for leaf_node in leaves:
            tasks_map[leaf_node.unique_id] >> test_task

    # Create the Airflow task dependencies between non-test nodes
    for node_id, node in nodes.items():
        for parent_node_id in node.depends_on:
            # depending on the node type, it will not have mapped 1:1 to tasks_map
            if (node_id in tasks_map) and (parent_node_id in tasks_map):
                tasks_map[parent_node_id] >> tasks_map[node_id]
