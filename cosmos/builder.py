from __future__ import annotations
import json
import logging
from dataclasses import dataclass
from subprocess import Popen, PIPE

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from cosmos.core.airflow import get_airflow_task as create_airflow_task
from cosmos.core.graph.entities import Task as TaskMetadata
from cosmos.render import calculate_operator_class


logger = logging.getLogger(__name__)


@dataclass
class DbtNode:
    name: str
    unique_id: str
    resource_type: str
    depends_on: list[str]
    file_path: str
    tags: list[str]


def extract_dbt_nodes(
    project_dir,
    profile_name="dbt_project.yml",
    resource_type=None,
    select=None,
    models=None,
    exclude=None,
    selector=None,
):
    # TODO: set these programmatically, take into account venv
    # - have a reliable way of checking if dbt is available
    # - support manifests

    # Have an alternative for K8s & Docker executor
    # command = ["dbt", "ls", "--exclude", "*orders*", "--output", "json", "--profiles-dir", project_dir]
    command = ["dbt", "ls", "--output", "json", "--profiles-dir", project_dir]

    process = Popen(command, stdout=PIPE, stderr=PIPE, cwd=project_dir)
    stdout, stderr = process.communicate()

    nodes = {}
    for line in stdout.decode().split("\n"):
        try:
            node_dict = json.loads(line.strip())
        except json.decoder.JSONDecodeError:
            print("Skipping line: %s", line)
        else:
            node = DbtNode(
                name=node_dict["name"],
                unique_id=node_dict["unique_id"],
                resource_type=node_dict["resource_type"],
                depends_on=node_dict["depends_on"].get("nodes", []),
                file_path=project_dir / node_dict["original_file_path"],
                tags=node_dict["tags"],
            )
            nodes[node.unique_id] = node

    return nodes


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


def create_test_task_metadata(test_task_name, execution_mode, task_args, model_name=None):
    if model_name is not None:
        task_args = {**task_args, **{"models": model_name}}
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
    task_group: TaskGroup | None = None,
):
    tasks_map = {}

    # usually, we'll try to map one DBT node to one Airflow task
    # the exception are the test nodes
    # if test_behaviour=="after_each", each model will be bundled with a test, using TaskGroup
    for node_id, node in nodes.items():
        task_meta = create_task_metadata(node=node, execution_mode=execution_mode, args=task_args)
        if task_meta and node.resource_type != "test":
            if node.resource_type == "model" and test_behavior == "after_each":
                with TaskGroup(dag=dag, group_id=node.name, parent_group=task_group) as model_task_group:
                    task = create_airflow_task(task_meta, dag, task_group=model_task_group)
                    test_meta = create_test_task_metadata(
                        f"{node.name}_test", execution_mode, task_args=task_args, model_name=node.unique_id
                    )
                    test_task = create_airflow_task(test_meta, dag, task_group=model_task_group)
                    task >> test_task
                    task_or_group = model_task_group
            else:
                task_or_group = create_airflow_task(task_meta, dag, task_group=task_group)
            tasks_map[node_id] = task_or_group

    # if test_behaviour=="after_all", there will be one test task, run "by the end" of the DAG, after the tasks which
    # are leaves, in other words, which do not have downstream tasks.
    if test_behavior == "after_all":
        test_meta = create_test_task_metadata(f"{dbt_project_name}_test", execution_mode, task_args=task_args)
        test_task = create_airflow_task(test_meta, dag, task_group=task_group)
        leaves = calculate_leaves(nodes)
        for leaf_node in leaves:
            tasks_map[leaf_node.unique_id] >> test_task

    # create the Airflow task dependencies between non-test nodes
    for node_id, node in nodes.items():
        for parent_node_id in node.depends_on:
            # depending on the node type, it will not have mapped 1:1 to tasks_map
            if (node_id in tasks_map) and (parent_node_id in tasks_map):
                tasks_map[parent_node_id] >> tasks_map[node_id]
