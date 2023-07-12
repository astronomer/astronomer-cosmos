from __future__ import annotations
import itertools
import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from subprocess import Popen, PIPE
from typing import Any

from cosmos.dbt.parser.project import DbtProject as LegacyDbtProject
from cosmos.dbt.project import DbtProject
from cosmos.dbt.selector import select_nodes

logger = logging.getLogger(__name__)

# TODO replace inline constants


class LoadMode(Enum):
    CUSTOM = "custom"
    DBT_LS = "dbt_ls"
    DBT_MANIFEST = "dbt_manifest"


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
    tags: list[str] = field(default_factory=lambda: [])
    config: dict[str, Any] = field(default_factory=lambda: {})


class DbtGraph:
    """
    Support loading a dbt project graph (represented by nodes) using different strategies.
    """

    nodes: dict[str, DbtNode] = dict()
    filtered_nodes: dict[str, DbtNode] = dict()

    def __init__(self, project: DbtProject, exclude=None, select=None, dbt_cmd="dbt"):
        self.project = project
        self.exclude = exclude or []
        self.select = select or []

        # specific to loading using ls
        self.dbt_cmd = dbt_cmd

    # TODO: implement smart load, which tries other load modes if the desired fails
    def load(self, method=LoadMode.CUSTOM):
        load_method = {
            LoadMode.CUSTOM: self.load_via_custom_parser,
            LoadMode.DBT_LS: self.load_via_dbt_ls,
            LoadMode.DBT_MANIFEST: self.load_from_dbt_manifest,
        }
        load_method[method]()

    def load_via_dbt_ls(self):
        command = [self.dbt_cmd, "ls", "--output", "json", "--profiles-dir", self.project.dir]
        if self.exclude:
            command.extend(["--exclude", *self.exclude])
        if self.select:
            command.extend(["--select", *self.select])

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
            dbt_models_dir=self.project.models_dir.stem,
            dbt_snapshots_dir=self.project.snapshots_dir.stem,
            dbt_seeds_dir=self.project.seeds_dir.stem,
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
                config=config,
            )
            nodes[model_name] = node

        self.nodes = nodes
        self.filtered_nodes = select_nodes(
            project_dir=self.project.dir, nodes=nodes, select=self.select, exclude=self.exclude
        )

    def load_from_dbt_manifest(self):
        nodes = {}
        with open(self.project.manifest) as fp:
            manifest = json.load(fp)

            for unique_id, node_dict in manifest.get("nodes", {}).items():
                node = DbtNode(
                    name=node_dict["name"],
                    unique_id=unique_id,
                    resource_type=node_dict["resource_type"],
                    depends_on=node_dict["depends_on"].get("nodes", []),
                    file_path=self.project.dir / node_dict["original_file_path"],
                    tags=node_dict["tags"],
                    config=node_dict["config"],
                )
                nodes[node.unique_id] = node

            self.nodes = nodes
            self.filtered_nodes = select_nodes(
                project_dir=self.project.dir, nodes=nodes, select=self.select, exclude=self.exclude
            )
