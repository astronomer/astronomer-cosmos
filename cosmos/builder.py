from __future__ import annotations
import json
from dataclasses import dataclass
from subprocess import Popen, PIPE

from airflow.models.dag import DAG

from cosmos.core.airflow import get_airflow_task as create_airflow_task
from cosmos.core.graph.entities import Task
from cosmos.render import calculate_operator_class


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


def create_task_metadata(node: DbtNode, execution_mode, args):
    dbt_resource_to_class = {"model": "DbtRun", "snapshot": "DbtSnapshot", "seed": "DbtSeed", "test": "DbtTest"}
    task_metadata = Task(
        id=node.name,  # TODO: make it consistent with the old way
        operator_class=calculate_operator_class(
            execution_mode=execution_mode, dbt_class=dbt_resource_to_class[node.resource_type]
        ),
        arguments=args,
    )
    return task_metadata


def create_task_args(node: DbtNode, execution_mode, project_dir, conn_id, profile_args):
    # TODO: adjust for non-local execution modes
    args = {}
    if execution_mode == "local":
        args["project_dir"] = project_dir
        args["conn_id"] = conn_id
        args["profile_args"] = profile_args
    return args


def add_airflow_entities(nodes: list[DbtNode], dag: DAG, execution_mode: str, project_dir, conn_id, profile_args):
    tasks_map = {}
    # TODO: support task groups
    # TODO: add crazy test logic before/after

    # create Airflow tasks
    for node_id, node in nodes.items():
        args = create_task_args(node, execution_mode, project_dir, conn_id, profile_args)
        task_meta = create_task_metadata(node=node, execution_mode=execution_mode, args=args)
        task = create_airflow_task(task_meta, dag)
        tasks_map[node_id] = task

    # define Airflow tasks dependencies
    for node_id, node in nodes.items():
        for parent_node_id in node.depends_on:
            # depending on selection filters, it may not be
            if parent_node_id in tasks_map:
                tasks_map[parent_node_id] >> tasks_map[node_id]
