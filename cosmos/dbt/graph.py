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

from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
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
from cosmos.dbt.parser.project import LegacyDbtProject
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


def create_symlinks(project_path: Path, tmp_dir: Path) -> None:
    """Helper function to create symlinks to the dbt project files."""
    ignore_paths = (DBT_LOG_DIR_NAME, DBT_TARGET_DIR_NAME, "dbt_packages", "profiles.yml")
    for child_name in os.listdir(project_path):
        if child_name not in ignore_paths:
            os.symlink(project_path / child_name, tmp_dir / child_name)


def run_command(command: list[str], tmp_dir: Path, env_vars: dict[str, str]) -> str:
    """Run a command in a subprocess, returning the stdout."""
    logger.info("Running command: `%s`", " ".join(command))
    logger.info("Environment variable keys: %s", env_vars.keys())
    process = Popen(
        command,
        stdout=PIPE,
        stderr=PIPE,
        cwd=tmp_dir,
        universal_newlines=True,
        env=env_vars,
    )
    stdout, stderr = process.communicate()
    returncode = process.returncode

    if 'Run "dbt deps" to install package dependencies' in stdout and command[1] == "ls":
        raise CosmosLoadDbtException(
            "Unable to run dbt ls command due to missing dbt_packages. Set RenderConfig.dbt_deps=True."
        )

    if returncode or "Error" in stdout:
        details = stderr or stdout
        raise CosmosLoadDbtException(f"Unable to run {command} due to the error:\n{details}")

    return stdout


def parse_dbt_ls_output(project_path: Path, ls_stdout: str) -> dict[str, DbtNode]:
    """Parses the output of `dbt ls` into a dictionary of `DbtNode` instances."""
    nodes = {}
    for line in ls_stdout.split("\n"):
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
                file_path=project_path / node_dict["original_file_path"],
                tags=node_dict["tags"],
                config=node_dict["config"],
            )
            nodes[node.unique_id] = node
            logger.debug("Parsed dbt resource `%s` of type `%s`", node.unique_id, node.resource_type)
    return nodes


class DbtGraph:
    """
    A dbt project graph (represented by `nodes` and `filtered_nodes`).
    Supports different ways of loading the `dbt` project into this representation.

    Different loading methods can result in different `nodes` and `filtered_nodes`.

    Example of how to use:

        dbt_graph = DbtGraph(
            project=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
            render_config=RenderConfig(exclude=["*orders*"], select=[]),
            dbt_cmd="/usr/local/bin/dbt"
        )
        dbt_graph.load(method=LoadMode.DBT_LS, execution_mode=ExecutionMode.LOCAL)
    """

    nodes: dict[str, DbtNode] = dict()
    filtered_nodes: dict[str, DbtNode] = dict()

    def __init__(
        self,
        project: ProjectConfig,
        render_config: RenderConfig = RenderConfig(),
        execution_config: ExecutionConfig = ExecutionConfig(),
        profile_config: ProfileConfig | None = None,
        dbt_cmd: str = get_system_dbt(),
        operator_args: dict[str, Any] | None = None,
    ):
        self.project = project
        self.render_config = render_config
        self.profile_config = profile_config
        self.execution_config = execution_config
        self.operator_args = operator_args or {}
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

        Fundamentally, there are two different execution paths
        There is automatic, and manual.
        """

        load_method = {
            LoadMode.CUSTOM: self.load_via_custom_parser,
            LoadMode.DBT_LS: self.load_via_dbt_ls,
            LoadMode.DBT_MANIFEST: self.load_from_dbt_manifest,
        }

        if method == LoadMode.AUTOMATIC:
            if self.project.is_manifest_available():
                self.load_from_dbt_manifest()
            else:
                if execution_mode == ExecutionMode.LOCAL and self.profile_config:
                    try:
                        self.load_via_dbt_ls()
                    except FileNotFoundError:
                        self.load_via_custom_parser()
                else:
                    self.load_via_custom_parser()
        else:
            load_method[method]()

    def run_dbt_ls(self, project_path: Path, tmp_dir: Path, env_vars: dict[str, str]) -> dict[str, DbtNode]:
        """Runs dbt ls command and returns the parsed nodes."""
        ls_command = [self.dbt_cmd, "ls", "--output", "json"]

        if self.render_config.exclude:
            ls_command.extend(["--exclude", *self.render_config.exclude])

        if self.render_config.select:
            ls_command.extend(["--select", *self.render_config.select])

        ls_command.extend(self.local_flags)

        stdout = run_command(ls_command, tmp_dir, env_vars)

        logger.debug("dbt ls output: %s", stdout)
        log_filepath = self.log_dir / DBT_LOG_FILENAME
        logger.debug("dbt logs available in: %s", log_filepath)
        if log_filepath.exists():
            with open(log_filepath) as logfile:
                for line in logfile:
                    logger.debug(line.strip())

        nodes = parse_dbt_ls_output(project_path, stdout)
        return nodes

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
        logger.info(f"Trying to parse the dbt project in `{self.render_config.project_path}` using dbt ls...")
        if not self.render_config.project_path or not self.execution_config.project_path:
            raise CosmosLoadDbtException(
                "Unable to load project via dbt ls without RenderConfig.dbt_project_path and ExecutionConfig.dbt_project_path"
            )

        if not self.profile_config:
            raise CosmosLoadDbtException("Unable to load project via dbt ls without a profile config.")

        if not shutil.which(self.dbt_cmd):
            raise CosmosLoadDbtException(f"Unable to find the dbt executable: {self.dbt_cmd}")

        with tempfile.TemporaryDirectory() as tmpdir:
            logger.info(
                f"Content of the dbt project dir {self.render_config.project_path}: `{os.listdir(self.render_config.project_path)}`"
            )
            tmpdir_path = Path(tmpdir)
            create_symlinks(self.render_config.project_path, tmpdir_path)

            with self.profile_config.ensure_profile(use_mock_values=True) as profile_values:
                (profile_path, env_vars) = profile_values
                env = os.environ.copy()
                env.update(env_vars)

                self.local_flags = [
                    "--project-dir",
                    str(tmpdir),
                    "--profiles-dir",
                    str(profile_path.parent),
                    "--profile",
                    self.profile_config.profile_name,
                    "--target",
                    self.profile_config.target_name,
                ]
                self.log_dir = Path(env.get(DBT_LOG_PATH_ENVVAR) or tmpdir_path / DBT_LOG_DIR_NAME)
                self.target_dir = Path(env.get(DBT_TARGET_PATH_ENVVAR) or tmpdir_path / DBT_TARGET_DIR_NAME)
                env[DBT_LOG_PATH_ENVVAR] = str(self.log_dir)
                env[DBT_TARGET_PATH_ENVVAR] = str(self.target_dir)

                if self.render_config.dbt_deps:
                    deps_command = [self.dbt_cmd, "deps"]
                    deps_command.extend(self.local_flags)
                    stdout = run_command(deps_command, tmpdir_path, env)
                    logger.debug("dbt deps output: %s", stdout)

                nodes = self.run_dbt_ls(self.execution_config.project_path, tmpdir_path, env)

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
        logger.info("Trying to parse the dbt project `%s` using a custom Cosmos method...", self.project.project_name)

        if not self.render_config.project_path or not self.execution_config.project_path:
            raise CosmosLoadDbtException(
                "Unable to load dbt project without RenderConfig.dbt_project_path and ExecutionConfig.dbt_project_path"
            )

        project = LegacyDbtProject(
            project_name=self.render_config.project_path.stem,
            dbt_root_path=self.render_config.project_path.parent.as_posix(),
            dbt_models_dir=self.project.models_path.stem if self.project.models_path else "models",
            dbt_seeds_dir=self.project.seeds_path.stem if self.project.seeds_path else "seeds",
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
                file_path=Path(
                    model.path.as_posix().replace(
                        self.render_config.project_path.as_posix(), self.execution_config.project_path.as_posix()
                    )
                ),
                tags=[],
                config=config,
            )
            nodes[model_name] = node

        self.nodes = nodes
        self.filtered_nodes = select_nodes(
            project_dir=self.execution_config.project_path,
            nodes=nodes,
            select=self.render_config.select,
            exclude=self.render_config.exclude,
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
        logger.info("Trying to parse the dbt project `%s` using a dbt manifest...", self.project.project_name)

        if not self.project.is_manifest_available():
            raise CosmosLoadDbtException(f"Unable to load manifest using {self.project.manifest_path}")

        if not self.execution_config.project_path:
            raise CosmosLoadDbtException("Unable to load manifest without ExecutionConfig.dbt_project_path")

        nodes = {}
        with open(self.project.manifest_path) as fp:  # type: ignore[arg-type]
            manifest = json.load(fp)

            resources = {**manifest.get("nodes", {}), **manifest.get("sources", {}), **manifest.get("exposures", {})}
            for unique_id, node_dict in resources.items():
                node = DbtNode(
                    name=node_dict.get("alias", node_dict["name"]),
                    unique_id=unique_id,
                    resource_type=DbtResourceType(node_dict["resource_type"]),
                    depends_on=node_dict.get("depends_on", {}).get("nodes", []),
                    file_path=self.execution_config.project_path / Path(node_dict["original_file_path"]),
                    tags=node_dict["tags"],
                    config=node_dict["config"],
                )

                nodes[node.unique_id] = node

            self.nodes = nodes
            self.filtered_nodes = select_nodes(
                project_dir=self.execution_config.project_path,
                nodes=nodes,
                select=self.render_config.select,
                exclude=self.render_config.exclude,
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
