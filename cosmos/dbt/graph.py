from __future__ import annotations
import itertools
import json
import logging
import os
from dataclasses import dataclass, field
from subprocess import Popen, PIPE
from typing import Any

from cosmos.constants import DbtResourceType, ExecutionMode, LoadMode
from cosmos.dbt.executable import get_system_dbt
from cosmos.dbt.parser.project import DbtProject as LegacyDbtProject
from cosmos.dbt.project import DbtProject
from cosmos.dbt.selector import select_nodes

logger = logging.getLogger(__name__)

# TODO replace inline constants


class CosmosLoadDbtException(Exception):
    """
    Exception raised while trying to load a `dbt` project as a `DbtGraph` instance.
    """

    pass


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
    A dbt project graph (represented by `nodes` and `filtered_nodes`).
    Supports different ways of loading the `dbt` project into this representation.

    Different loading methods can result in different `nodes` and `filtered_nodes`.

    Example of how to use:

        dbt_graph = DbtGraph(
            project=DbtProject(name="jaffle_shop", root_dir=DBT_PROJECTS_ROOT_DIR),
            exclude=["*orders*"],
            select=[],
            dbt_cmd="/usr/local/bin/dbt",
        )
        dbt_graph.load(method=LoadMode.DBT_LS, execution_mode=ExecutionMode.LOCAL)
    """

    nodes: dict[str, DbtNode] = dict()
    filtered_nodes: dict[str, DbtNode] = dict()

    def __init__(
        self,
        project: DbtProject,
        exclude: list[str] | None = None,
        select: list[str] = None,
        dbt_cmd: str = get_system_dbt(),
    ):
        self.project = project
        self.exclude = exclude or []
        self.select = select or []

        # specific to loading using ls
        self.dbt_cmd = dbt_cmd

    def load(self, method: LoadMode = LoadMode.AUTOMATIC, execution_mode: ExecutionMode = ExecutionMode.LOCAL) -> None:
        """
        Load a `dbt` project into a `DbtGraph`, setting `nodes` and `filtered_nodes` accordingly.

        :param method: How to load `nodes` from a `dbt` project (automatically, using custom parser, using dbt manifest
            or dbt ls)
        :param execution_mode: Where Cosmos should run each dbt task (e.g. ExecutionMode.KUBERNETES)
        """
        load_method = {
            LoadMode.CUSTOM: self.load_via_custom_parser,
            LoadMode.DBT_LS: self.load_via_dbt_ls,
            LoadMode.DBT_MANIFEST: self.load_from_dbt_manifest,
        }
        if method == LoadMode.AUTOMATIC:
            if self.project.is_manifest_available():
                self.load_from_dbt_manifest()
                return
            elif (
                execution_mode in (ExecutionMode.LOCAL, ExecutionMode.VIRTUALENV)
                and self.project.is_profile_yml_available()
            ):
                try:
                    self.load_via_dbt_ls()
                    return
                except FileNotFoundError:
                    self.load_via_custom_parser()
                    return
            else:
                self.load_via_custom_parser()
                return

        if method == LoadMode.DBT_MANIFEST and not self.project.is_manifest_available():
            raise CosmosLoadDbtException(f"Unable to load manifest using {self.project.manifest_path}")

        load_method[method]()

    def load_via_dbt_ls(self):
        """
        This is the most accurate way of loading `dbt` projects and filtering them out, since it uses the `dbt` command
        line for both parsing and filtering the nodes.

        Updates in-place:
        * self.nodes
        * self.filtered_nodes
        """
        logger.info("Trying to parse the dbt project using dbt ls...")
        command = [self.dbt_cmd, "ls", "--output", "json", "--profiles-dir", self.project.dir]
        if self.exclude:
            command.extend(["--exclude", *self.exclude])
        if self.select:
            command.extend(["--select", *self.select])
        logger.info(f"Running command: {command}")
        try:
            process = Popen(
                command, stdout=PIPE, stderr=PIPE, cwd=self.project.dir, universal_newlines=True, env=os.environ
            )
        except FileNotFoundError as exception:
            raise CosmosLoadDbtException(f"Unable to run the command due to the error:\n{exception}")

        stdout, stderr = process.communicate()

        logger.debug(f"Output: {stdout}")

        if stderr or "Runtime Error" in stdout:
            details = stderr or stdout
            raise CosmosLoadDbtException(f"Unable to run the command due to the error:\n{details}")

        nodes = {}
        for line in stdout.split("\n"):
            try:
                node_dict = json.loads(line.strip())
            except json.decoder.JSONDecodeError:
                logger.info("Skipping line: %s", line)
            else:
                node = DbtNode(
                    name=node_dict["name"],
                    unique_id=node_dict["unique_id"],
                    resource_type=DbtResourceType(node_dict["resource_type"]),
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
        This is the least accurate way of loading `dbt` projects and filtering them out, since it uses custom Cosmos
        logic, which is usually a subset of what is available in `dbt`.

        Internally, it uses the legacy Cosmos DbtProject representation and converts it to the current
        nodes list representation.

        Updates in-place:
        * self.nodes
        * self.filtered_nodes
        """
        logger.info("Trying to parse the dbt project using a custom Cosmos method...")
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
                resource_type=DbtResourceType(model.type.value),
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
        """
        This approach accurately loads `dbt` projects using the `manifest.yml` file.

        However, since the Manifest does not represent filters, it relies on the Custom Cosmos implementation
        to filter out the nodes relevant to the user (based on self.exclude and self.select).

        Updates in-place:
        * self.nodes
        * self.filtered_nodes
        """
        logger.info("Trying to parse the dbt project using a dbt manifest...")
        nodes = {}
        with open(self.project.manifest_path) as fp:
            manifest = json.load(fp)

            for unique_id, node_dict in manifest.get("nodes", {}).items():
                node = DbtNode(
                    name=node_dict["name"],
                    unique_id=unique_id,
                    resource_type=DbtResourceType(node_dict["resource_type"]),
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
