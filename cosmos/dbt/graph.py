import itertools
import json
import logging
from dataclasses import dataclass
from subprocess import Popen, PIPE
from typing import Any

from cosmos.dbt.parser.project import DbtProject as LegacyDbtProject
from cosmos.dbt.project import DbtProject
from cosmos.dbt.filter import filter_nodes

logger = logging.getLogger(__name__)

# TODO replace inline constants


@dataclass
class DbtNode:
    """
    Metadata related to a dbt node (e.g. model, seed, snapshot).
    """

    name: str
    unique_id: str
    resource_type: str
    depends_on: list[str]
    file_path: str
    tags: list[str]
    config: dict[str, Any]


class DbtGraph:
    """
    Support loading a dbt project graph (represented by nodes) using different strategies.
    """

    nodes: list[DbtNode] = []
    filtered_nodes: list[DbtNode] = []

    def __init__(self, project: DbtProject, exclude=None, select=None, dbt_cmd="dbt"):
        self.project = project
        self.exclude = exclude or {}
        self.select = select or {}

        # specific to loading using ls
        self.dbt_cmd = dbt_cmd

    def load(self):
        # TODO: defined order of precedence and criteria to use one or another method
        self.load_via_custom_parser()
        self.load_via_dbt_ls()
        # self.load_from_dbt_manifest()

    def load_via_dbt_ls(self):
        command = [self.dbt_cmd, "ls", "--output", "json", "--profiles-dir", self.project.dir]
        if self.exclude:
            command.extend(["--exclude", ",".join(self.exclude)])
        if self.select:
            command.extend(["--select", ",".join(self.select)])

        process = Popen(command, stdout=PIPE, stderr=PIPE, cwd=self.project.dir)
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
                    file_path=self.project.dir / node_dict["original_file_path"],
                    tags=node_dict["tags"],
                    config=node_dict["config"],
                )
                nodes[node.unique_id] = node

        self.nodes = nodes
        self.filtered_nodes = nodes

    def load_via_custom_parser(self):
        """
        Convert from the legacy Cosmos DbtProject to the new list of nodes representation.
        """
        project = LegacyDbtProject(
            dbt_root_path=self.project.root_dir,
            dbt_models_dir=self.project.models_dir,
            dbt_snapshots_dir=self.project.snapshots_dir,
            dbt_seeds_dir=self.project.seeds_dir,
            project_name=self.project.name,
        )
        nodes = {}
        models = itertools.chain(project.models.items(), project.snapshots.items(), project.seeds.items())
        for model_name, model in models:
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

        self.nodes = nodes

        self.filtered_nodes = filter_nodes(
            project_dir=self.project.dir, nodes=nodes, select=self.select, exclude=self.exclude
        )

    def load_via_manifest(self):
        # TODO
        pass
