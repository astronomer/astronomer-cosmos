"Contains the DbtGraph class which is used to represent a dbt project graph."
from __future__ import annotations

import itertools
import json
import logging
from subprocess import Popen, PIPE

from cosmos.constants import DbtResourceType, ExecutionMode, LoadMode
from cosmos.config import CosmosConfig
from cosmos.dbt.parser.project import DbtProject as LegacyDbtProject
from cosmos.dbt.selector import select_nodes
from cosmos.dbt.node import DbtNode

logger = logging.getLogger(__name__)


class CosmosLoadDbtException(Exception):
    """
    Exception raised while trying to load a `dbt` project as a `DbtGraph` instance.
    """


class DbtGraph:
    """
    A dbt project graph (represented by `nodes` and `filtered_nodes`).
    Supports different ways of loading the `dbt` project into this representation.

    Different loading methods can result in different `nodes` and `filtered_nodes`.

    Example of how to use:

        dbt_graph = DbtGraph(
            project_config=ProjectConfig(...),
            render_config=RenderConfig(...),
            profile_config=ProfileConfig(...),
            dbt_cmd="/usr/local/bin/dbt",
        )
        dbt_graph.load()
    """

    nodes: dict[str, DbtNode] = dict()
    filtered_nodes: dict[str, DbtNode] = dict()

    def __init__(
        self,
        cosmos_config: CosmosConfig,
    ):
        self.project_config = cosmos_config.project_config
        self.render_config = cosmos_config.render_config
        self.profile_config = cosmos_config.profile_config
        self.execution_config = cosmos_config.execution_config

    def load(self) -> None:
        """
        Load a `dbt` project into a `DbtGraph`, setting `nodes` and `filtered_nodes` accordingly.
        """
        if self.render_config.load_method_enum == LoadMode.AUTOMATIC:
            if self.project_config.is_manifest_available():
                return self.load_from_dbt_manifest()

            if self.execution_config.execution_mode_enum in (ExecutionMode.LOCAL, ExecutionMode.VIRTUALENV):
                try:
                    return self.load_via_dbt_ls()
                except FileNotFoundError:
                    return self.load_via_custom_parser()

            return self.load_via_custom_parser()

        {
            LoadMode.CUSTOM: self.load_via_custom_parser,
            LoadMode.DBT_LS: self.load_via_dbt_ls,
            LoadMode.DBT_MANIFEST: self.load_from_dbt_manifest,
        }[self.render_config.load_method_enum]()

        if method == LoadMode.DBT_MANIFEST and not self.project.is_manifest_available():
            raise CosmosLoadDbtException(f"Unable to load manifest using {self.project.manifest_path}")

        load_method[method]()

    def load_via_dbt_ls(self) -> None:
        """
        This is the most accurate way of loading `dbt` projects and filtering them out, since it uses the `dbt` command
        line for both parsing and filtering the nodes.

        Updates in-place:
        * self.nodes
        * self.filtered_nodes
        """
        logger.info("Trying to parse the dbt project using dbt ls...")

        command = [
            str(self.execution_config.dbt_executable_path),
            "ls",
            "--output",
            "json",
            "--profiles-dir",
            str(self.project_config.dbt_project_path),
        ]

        if self.render_config.exclude:
            command.extend(["--exclude", *self.render_config.exclude])

        if self.render_config.select:
            command.extend(["--select", *self.render_config.select])

        logger.info("Running command `%s`", command)
        process = Popen(
            command,
            stdout=PIPE,
            stderr=PIPE,
            cwd=self.project_config.dbt_project_path,
            universal_newlines=True,
        )

        stdout, stderr = process.communicate()

        logger.debug("Command output: %s", stdout)

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
                node_id = node_dict["unique_id"]
                nodes[node_id] = DbtNode(
                    name=node_dict["name"],
                    unique_id=node_id,
                    resource_type=DbtResourceType(node_dict["resource_type"]),
                    depends_on=node_dict["depends_on"].get("nodes", []),
                    file_path=self.project_config.dbt_project_path / node_dict["original_file_path"],
                    tags=node_dict["tags"],
                    config=node_dict["config"],
                )

        self.nodes = nodes
        self.filtered_nodes = nodes

    def load_via_custom_parser(self) -> None:
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
            dbt_root_path=str(self.project_config.dbt_project_path.parent),
            project_name=self.project_config.dbt_project_path.stem,
            dbt_models_dir=self.project_config.models_path.stem,
            dbt_snapshots_dir=self.project_config.snapshots_path.stem,
            dbt_seeds_dir=self.project_config.seeds_path.stem,
        )

        nodes = {}

        all_nodes = itertools.chain(project.models.items(), project.snapshots.items(), project.seeds.items())

        for node_name, node in all_nodes:
            config = {item.split(":")[0]: item.split(":")[-1] for item in node.config.config_selectors}
            nodes[node_name] = DbtNode(
                name=node_name,
                unique_id=node_name,
                resource_type=DbtResourceType(node.type.value),
                depends_on=list(node.config.upstream_models),
                file_path=str(node.path),
                tags=[],
                config=config,
            )

        self.nodes = nodes
        self.filtered_nodes = select_nodes(
            project_dir=self.project_config.dbt_project_path,
            nodes=nodes,
            select=self.render_config.select,
            exclude=self.render_config.exclude,
        )

    def load_from_dbt_manifest(self) -> None:
        """
        This approach accurately loads `dbt` projects using the `manifest.yml` file.

        However, since the Manifest does not represent filters, it relies on the Custom Cosmos implementation
        to filter out the nodes relevant to the user (based on self.exclude and self.select).

        Updates in-place:
        * self.nodes
        * self.filtered_nodes
        """
        if not self.project_config.is_manifest_available():
            raise ValueError(f"Unable to load manifest using {self.project_config.manifest_path}")

        logger.info("Trying to parse the dbt project using a dbt manifest...")
        nodes = {}
        with self.project_config.manifest_path.open() as manifest_file:
            manifest = json.load(manifest_file)

            for unique_id, node_dict in manifest.get("nodes", {}).items():
                node = DbtNode(
                    name=node_dict["name"],
                    unique_id=unique_id,
                    resource_type=DbtResourceType(node_dict["resource_type"]),
                    depends_on=node_dict["depends_on"].get("nodes", []),
                    file_path=self.project_config.dbt_project_path / node_dict["original_file_path"],
                    tags=node_dict["tags"],
                    config=node_dict["config"],
                )
                nodes[node.unique_id] = node

            self.nodes = nodes
            self.filtered_nodes = select_nodes(
                project_dir=self.project_config.dbt_project_path,
                nodes=nodes,
                select=self.render_config.select,
                exclude=self.render_config.exclude,
            )
