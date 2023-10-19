"""Module that contains all Cosmos config classes."""

from __future__ import annotations

import contextlib
import tempfile
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Any, Iterator, Callable

from cosmos.constants import DbtResourceType, TestBehavior, ExecutionMode, LoadMode
from cosmos.dbt.executable import get_system_dbt
from cosmos.exceptions import CosmosValueError
from cosmos.log import get_logger
from cosmos.profiles import BaseProfileMapping

logger = get_logger(__name__)

DEFAULT_PROFILES_FILE_NAME = "profiles.yml"


@dataclass
class RenderConfig:
    """
    Class for setting general Cosmos config.

    :param emit_datasets: If enabled model nodes emit Airflow Datasets for downstream cross-DAG
    dependencies. Defaults to True
    :param test_behavior: The behavior for running tests. Defaults to after each (model)
    :param load_method: The parsing method for loading the dbt model. Defaults to AUTOMATIC
    :param select: A list of dbt select arguments (e.g. 'config.materialized:incremental')
    :param exclude: A list of dbt exclude arguments (e.g. 'tag:nightly')
    :param dbt_deps: Configure to run dbt deps when using dbt ls for dag parsing
    """

    emit_datasets: bool = True
    test_behavior: TestBehavior = TestBehavior.AFTER_EACH
    load_method: LoadMode = LoadMode.AUTOMATIC
    select: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    dbt_deps: bool = True
    node_converters: dict[DbtResourceType, Callable[..., Any]] | None = None


@dataclass
class ProjectConfig:
    """
    Class for setting project config.

    :param dbt_project_path: The path to the dbt project directory. Example: /path/to/dbt/project. Defaults to None
    :param models_relative_path: The relative path to the dbt models directory within the project. Defaults to models
    :param seeds_relative_path: The relative path to the dbt seeds directory within the project. Defaults to seeds
    :param snapshots_relative_path: The relative path to the dbt snapshots directory within the project. Defaults to
    snapshots
    :param manifest_path: The absolute path to the dbt manifest file. Defaults to None
    :param project_name: Allows the user to define the project name.
    Required if dbt_project_path is not defined. Defaults to the folder name of dbt_project_path.
    """

    dbt_project_path: str | Path | None = None
    models_relative_path: str | Path = "models"
    seeds_relative_path: str | Path = "seeds"
    snapshots_relative_path: str | Path = "snapshots"
    manifest_path: str | Path | None = None
    project_name: str | None = None

    @cached_property
    def parsed_dbt_project_path(self) -> Path | None:
        return Path(self.dbt_project_path) if self.dbt_project_path else None

    @cached_property
    def parsed_manifest_path(self) -> Path | None:
        return Path(self.manifest_path) if self.manifest_path else None

    def __post_init__(self) -> None:
        "Converts paths to `Path` objects."
        if self.parsed_dbt_project_path:
            self.models_relative_path = self.parsed_dbt_project_path / Path(self.models_relative_path)
            self.seeds_relative_path = self.parsed_dbt_project_path / Path(self.seeds_relative_path)
            self.snapshots_relative_path = self.parsed_dbt_project_path / Path(self.snapshots_relative_path)
            if not self.project_name:
                self.project_name = self.parsed_dbt_project_path.stem

    def validate_project(self) -> None:
        """
        Validates necessary context is present for a project.
        There are 2 cases we need to account for
          1 - the entire dbt project
          2 - the dbt manifest
        Here, we can assume if the project path is provided, we have scenario 1.
        If the project path is not provided, we have a scenario 2
        """

        mandatory_paths = {}

        if self.parsed_dbt_project_path:
            project_yml_path = self.parsed_dbt_project_path / "dbt_project.yml"
            mandatory_paths = {
                "dbt_project.yml": project_yml_path,
                "models directory ": self.models_relative_path,
            }
        elif self.parsed_manifest_path:
            if not self.project_name:
                raise CosmosValueError(
                    "project_name required when manifest_path is present and dbt_project_path is not."
                )
            mandatory_paths = {"manifest file": self.parsed_manifest_path}
        else:
            raise CosmosValueError("dbt_project_path or manifest_path are required parameters.")

        for name, path in mandatory_paths.items():
            if path is None or not Path(path).exists():
                raise CosmosValueError(f"Could not find {name} at {path}")

    def is_manifest_available(self) -> bool:
        """
        Check if the `dbt` project manifest is set and if the file exists.
        """
        if not self.parsed_manifest_path:
            return False

        return self.parsed_manifest_path.exists()


@dataclass
class ProfileConfig:
    """
    Class for setting profile config. Supports two modes of operation:
    1. Using a user-supplied profiles.yml file. If using this mode, set profiles_yml_filepath to the
    path to the file.
    2. Using cosmos to map Airflow connections to dbt profiles. If using this mode, set
    profile_mapping to a subclass of BaseProfileMapping.

    :param profile_name: The name of the dbt profile to use.
    :param target_name: The name of the dbt target to use.
    :param profiles_yml_filepath: The path to a profiles.yml file to use.
    :param profile_mapping: A mapping of Airflow connections to dbt profiles.
    """

    # should always be set to be explicit
    profile_name: str
    target_name: str

    # should be set if using a user-supplied profiles.yml
    profiles_yml_filepath: str | Path | None = None

    # should be set if using cosmos to map Airflow connections to dbt profiles
    profile_mapping: BaseProfileMapping | None = None

    def __post_init__(self) -> None:
        "Validates that we have enough information to render a profile."
        # if using a user-supplied profiles.yml, validate that it exists
        if self.profiles_yml_filepath and not Path(self.profiles_yml_filepath).exists():
            raise CosmosValueError(f"The file {self.profiles_yml_filepath} does not exist.")

    def validate_profile(self) -> None:
        "Validates that we have enough information to render a profile."
        if not self.profiles_yml_filepath and not self.profile_mapping:
            raise CosmosValueError("Either profiles_yml_filepath or profile_mapping must be set to render a profile")

    @contextlib.contextmanager
    def ensure_profile(
        self, desired_profile_path: Path | None = None, use_mock_values: bool = False
    ) -> Iterator[tuple[Path, dict[str, str]]]:
        "Context manager to ensure that there is a profile. If not, create one."
        if self.profiles_yml_filepath:
            logger.info("Using user-supplied profiles.yml at %s", self.profiles_yml_filepath)
            yield Path(self.profiles_yml_filepath), {}

        elif self.profile_mapping:
            profile_contents = self.profile_mapping.get_profile_file_contents(
                profile_name=self.profile_name, target_name=self.target_name, use_mock_values=use_mock_values
            )

            if use_mock_values:
                env_vars = {}
            else:
                env_vars = self.profile_mapping.env_vars

            if desired_profile_path:
                logger.info(
                    "Writing profile to %s with the following contents:\n%s",
                    desired_profile_path,
                    profile_contents,
                )
                # write profile_contents to desired_profile_path using yaml library
                desired_profile_path.write_text(profile_contents)
                yield desired_profile_path, env_vars
            else:
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_file = Path(temp_dir) / DEFAULT_PROFILES_FILE_NAME
                    logger.info(
                        "Creating temporary profiles.yml at %s with the following contents:\n%s",
                        temp_file,
                        profile_contents,
                    )
                    temp_file.write_text(profile_contents)
                    yield temp_file, env_vars


@dataclass
class ExecutionConfig:
    """
    Contains configuration about how to execute dbt.

    :param execution_mode: The execution mode for dbt. Defaults to local
    :param dbt_executable_path: The path to the dbt executable. Defaults to dbt if
    available on the path.
    """

    execution_mode: ExecutionMode = ExecutionMode.LOCAL
    dbt_executable_path: str | Path = get_system_dbt()
