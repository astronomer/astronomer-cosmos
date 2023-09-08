from __future__ import annotations

import itertools
import json
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from subprocess import PIPE, Popen
from typing import Any

from cosmos.config import ProfileConfig
from cosmos.constants import (
    DBT_LOG_DIR_NAME,
    DBT_LOG_FILENAME,
    DBT_LOG_PATH_ENVVAR,
    DBT_TARGET_DIR_NAME,
    DBT_TARGET_PATH_ENVVAR,
    DbtResourceType,
    ExecutionMode,
    LoadMode,
)
from cosmos.dbt.executable import get_system_dbt
from cosmos.dbt.parser.project import DbtProject as LegacyDbtProject
from cosmos.dbt.project import DbtProject
from cosmos.dbt.selector import select_nodes
from cosmos.log import get_logger

logger = get_logger(__name__)


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
    resource_type: DbtResourceType
    depends_on: list[str]
    file_path: Path
    tags: list[str] = field(default_factory=lambda: [])
    config: dict[str, Any] = field(default_factory=lambda: {})
    has_test: bool = False


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
        select: list[str] | None = None,
        dbt_cmd: str = get_system_dbt(),
        profile_config: ProfileConfig | None = None,
        operator_args: dict[str, Any] | None = None,
        dbt_deps: bool | None = True,
    ):
        self.project = project
        self.exclude = exclude or []
        self.select = select or []
        self.profile_config = profile_config
        self.operator_args = operator_args or {}
        self.dbt_deps = dbt_deps

        # specific to loading using ls
        self.dbt_deps = dbt_deps
        self.dbt_cmd = dbt_cmd

    def load(
        self,
        method: LoadMode = LoadMode.AUTOMATIC,
        execution_mode: ExecutionMode = ExecutionMode.LOCAL,
    ) -> None:
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
            elif execution_mode == ExecutionMode.LOCAL and self.project.is_profile_yml_available():
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

    def load_via_dbt_ls(self) -> None:
        """
        This is the most accurate way of loading `dbt` projects and filtering them out, since it uses the `dbt` command
        line for both parsing and filtering the nodes.

        Noted that if dbt project contains versioned models, need to use dbt>=1.6.0 instead. Because, as dbt<1.6.0,
        dbt cli doesn't support select a specific versioned models as stg_customers_v1, customers_v1, ...

        Updates in-place:
        * self.nodes
        * self.filtered_nodes
        """
        logger.info("Trying to parse the dbt project `%s` in `%s` using dbt ls...", self.project.name, self.project.dir)

        if not self.profile_config:
            raise CosmosLoadDbtException("Unable to load dbt project without a profile config")

        if not shutil.which(self.dbt_cmd):
            raise CosmosLoadDbtException(f"Unable to find the dbt executable: {self.dbt_cmd}")

        with self.profile_config.ensure_profile(use_mock_values=True) as profile_values:
            (profile_path, env_vars) = profile_values
            env = os.environ.copy()
            env.update(env_vars)

            with tempfile.TemporaryDirectory() as tmpdir:
                logger.info("Content of the dbt project dir <%s>: `%s`", self.project.dir, os.listdir(self.project.dir))
                logger.info("Creating symlinks from %s to `%s`", self.project.dir, tmpdir)
                # We create symbolic links to the original directory files and directories.
                # This allows us to run the dbt command from within the temporary directory, outputting any necessary
                # artifact and also allow us to run `dbt deps`
                tmpdir_path = Path(tmpdir)
                ignore_paths = (DBT_LOG_DIR_NAME, DBT_TARGET_DIR_NAME, "profiles.yml")
                for child_name in os.listdir(self.project.dir):
                    if child_name not in ignore_paths:
                        os.symlink(self.project.dir / child_name, tmpdir_path / child_name)

                local_flags = [
                    "--project-dir",
                    str(tmpdir),
                    "--profiles-dir",
                    str(profile_path.parent),
                    "--profile",
                    self.profile_config.profile_name,
                    "--target",
                    self.profile_config.target_name,
                ]
                log_dir = Path(env.get(DBT_LOG_PATH_ENVVAR) or tmpdir_path / DBT_LOG_DIR_NAME)
                target_dir = Path(env.get(DBT_TARGET_PATH_ENVVAR) or tmpdir_path / DBT_TARGET_DIR_NAME)
                env[DBT_LOG_PATH_ENVVAR] = str(log_dir)
                env[DBT_TARGET_PATH_ENVVAR] = str(target_dir)

                if self.dbt_deps:
                    deps_command = [self.dbt_cmd, "deps"]
                    deps_command.extend(local_flags)
                    logger.info("Running command: `%s`", " ".join(deps_command))
                    logger.info("Environment variable keys: %s", env.keys())
                    process = Popen(
                        deps_command,
                        stdout=PIPE,
                        stderr=PIPE,
                        cwd=tmpdir,
                        universal_newlines=True,
                        env=env,
                    )
                    stdout, stderr = process.communicate()
                    returncode = process.returncode
                    logger.debug("dbt deps output: %s", stdout)

                    if returncode or "Error" in stdout:
                        details = stderr or stdout
                        raise CosmosLoadDbtException(f"Unable to run dbt deps command due to the error:\n{details}")

                ls_command = [self.dbt_cmd, "ls", "--output", "json"]

                if self.exclude:
                    ls_command.extend(["--exclude", *self.exclude])

                if self.select:
                    ls_command.extend(["--select", *self.select])

                ls_command.extend(local_flags)

                logger.info("Running command: `%s`", " ".join(ls_command))
                logger.info("Environment variable keys: %s", env.keys())

                process = Popen(
                    ls_command,
                    stdout=PIPE,
                    stderr=PIPE,
                    cwd=tmpdir,
                    universal_newlines=True,
                    env=env,
                )

                stdout, stderr = process.communicate()
                returncode = process.returncode

                logger.debug("dbt output: %s", stdout)
                log_filepath = log_dir / DBT_LOG_FILENAME
                logger.debug("dbt logs available in: %s", log_filepath)
                if log_filepath.exists():
                    with open(log_filepath) as logfile:
                        for line in logfile:
                            logger.debug(line.strip())

                if 'Run "dbt deps" to install package dependencies' in stdout:
                    raise CosmosLoadDbtException(
                        "Unable to run dbt ls command due to missing dbt_packages. Set render_config.dbt_deps=True."
                    )

                if returncode or "Error" in stdout:
                    details = stderr or stdout
                    raise CosmosLoadDbtException(f"Unable to run dbt ls command due to the error:\n{details}")

                nodes = {}
                for line in stdout.split("\n"):
                    try:
                        node_dict = json.loads(line.strip())
                    except json.decoder.JSONDecodeError:
                        logger.debug("Skipped dbt ls line: %s", line)
                    else:
                        node = DbtNode(
                            name=node_dict.get("alias", node_dict["name"]),
                            unique_id=node_dict["unique_id"],
                            resource_type=DbtResourceType(node_dict["resource_type"]),
                            depends_on=node_dict.get("depends_on", {}).get("nodes", []),
                            file_path=self.project.dir / node_dict["original_file_path"],
                            tags=node_dict["tags"],
                            config=node_dict["config"],
                        )
                        nodes[node.unique_id] = node
                        logger.debug("Parsed dbt resource `%s` of type `%s`", node.unique_id, node.resource_type)

                self.nodes = nodes
                self.filtered_nodes = nodes

        self.update_node_dependency()

        logger.info("Total nodes: %i", len(self.nodes))
        logger.info("Total filtered nodes: %i", len(self.nodes))

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
        logger.info("Trying to parse the dbt project `%s` using a custom Cosmos method...", self.project.name)

        project = LegacyDbtProject(
            dbt_root_path=str(self.project.root_dir),
            dbt_models_dir=self.project.models_dir.stem if self.project.models_dir else None,
            dbt_seeds_dir=self.project.seeds_dir.stem if self.project.seeds_dir else None,
            project_name=self.project.name,
            operator_args=self.operator_args,
        )
        nodes = {}
        models = itertools.chain(
            project.models.items(), project.snapshots.items(), project.seeds.items(), project.tests.items()
        )
        for model_name, model in models:
            config = {item.split(":")[0]: item.split(":")[-1] for item in model.config.config_selectors}
            node = DbtNode(
                name=model_name,
                unique_id=model_name,
                resource_type=DbtResourceType(model.type.value),
                depends_on=list(model.config.upstream_models),
                file_path=model.path,
                tags=[],
                config=config,
            )
            nodes[model_name] = node

        self.nodes = nodes
        self.filtered_nodes = select_nodes(
            project_dir=self.project.dir, nodes=nodes, select=self.select, exclude=self.exclude
        )

        self.update_node_dependency()

        logger.info("Total nodes: %i", len(self.nodes))
        logger.info("Total filtered nodes: %i", len(self.nodes))

    def load_from_dbt_manifest(self) -> None:
        """
        This approach accurately loads `dbt` projects using the `manifest.yml` file.

        However, since the Manifest does not represent filters, it relies on the Custom Cosmos implementation
        to filter out the nodes relevant to the user (based on self.exclude and self.select).

        Noted that if dbt project contains versioned models, need to use dbt>=1.6.0 instead. Because, as dbt<1.6.0,
        dbt cli doesn't support select a specific versioned models as stg_customers_v1, customers_v1, ...

        Updates in-place:
        * self.nodes
        * self.filtered_nodes
        """
        logger.info("Trying to parse the dbt project `%s` using a dbt manifest...", self.project.name)
        nodes = {}
        with open(self.project.manifest_path) as fp:  # type: ignore[arg-type]
            manifest = json.load(fp)

            for unique_id, node_dict in manifest.get("nodes", {}).items():
                node = DbtNode(
                    name=node_dict.get("alias", node_dict["name"]),
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

            self.update_node_dependency()

        logger.info("Total nodes: %i", len(self.nodes))
        logger.info("Total filtered nodes: %i", len(self.nodes))

    def update_node_dependency(self) -> None:
        """
        This will update the property `has_text` if node has `dbt` test

        Updates in-place:
        * self.filtered_nodes
        """
        for _, node in self.filtered_nodes.items():
            if node.resource_type == DbtResourceType.TEST:
                for node_id in node.depends_on:
                    if node_id in self.filtered_nodes:
                        self.filtered_nodes[node_id].has_test = True
