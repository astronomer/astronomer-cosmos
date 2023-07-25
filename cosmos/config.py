"""Module that contains all Cosmos config classes."""

from __future__ import annotations

import shutil
import contextlib
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from logging import getLogger
from typing import Iterator

from cosmos.constants import TestBehavior, ExecutionMode, LoadMode
from cosmos.exceptions import CosmosValueError
from cosmos.profiles import BaseProfileMapping

logger = getLogger(__name__)


@dataclass
class RenderConfig:
    """
    Class for setting general Cosmos config.

    :param emit_datasets: If enabled test nodes emit Airflow Datasets for downstream cross-DAG
    dependencies. Defaults to True
    :param test_behavior: The behavior for running tests. Defaults to after each
    :param load_method: The parsing method for loading the dbt model. Defaults to AUTOMATIC
    :param select: A list of dbt select arguments (e.g. 'config.materialized:incremental')
    :param exclude: A list of dbt exclude arguments (e.g. 'tag:nightly')
    """

    emit_datasets: bool = True
    test_behavior: TestBehavior = TestBehavior.AFTER_EACH
    load_method: LoadMode = LoadMode.AUTOMATIC
    select: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)


@dataclass
class ProjectConfig:
    """
    Class for setting project config.

    :param dbt_project_path: The path to the dbt project directory. Example: /path/to/dbt/project
    :param models_dir: The path to the dbt models directory within the project. Defaults to models
    :param seeds_dir: The path to the dbt seeds directory within the project. Defaults to seeds
    :param snapshots_dir: The path to the dbt snapshots directory within the project. Defaults to
    snapshots
    :param manifest: The path to the dbt manifest file. Defaults to None
    """

    dbt_project: str | Path
    models: str | Path = "models"
    seeds: str | Path = "seeds"
    snapshots: str | Path = "snapshots"
    manifest: str | Path | None = None

    manifest_path: Path | None = None

    def __post_init__(self) -> None:
        "Converts paths to `Path` objects."
        self.dbt_project_path = Path(self.dbt_project)
        self.models_path = self.dbt_project_path / Path(self.models)
        self.seeds_path = self.dbt_project_path / Path(self.seeds)
        self.snapshots_path = self.dbt_project_path / Path(self.snapshots)

        if self.manifest:
            self.manifest_path = Path(self.manifest)

    def validate_project(self) -> None:
        "Validates that the project, models, and seeds directories exist."
        project_yml_path = self.dbt_project_path / "dbt_project.yml"
        if not project_yml_path.exists():
            raise CosmosValueError(f"Could not find dbt_project.yml at {project_yml_path}")

        if not self.models_path.exists():
            raise CosmosValueError(f"Could not find models directory at {self.models_path}")

        if self.manifest_path and not self.manifest_path.exists():
            raise CosmosValueError(f"Could not find manifest at {self.manifest_path}")

    def is_manifest_available(self) -> bool:
        """
        Check if the `dbt` project manifest is set and if the file exists.
        """
        if not self.manifest_path:
            return False

        return self.manifest_path.exists()

    @property
    def project_name(self) -> str:
        "The name of the dbt project."
        return self.dbt_project_path.stem


@dataclass
class ProfileConfig:
    """
    Class for setting profile config. Supports two modes of operation:
    1. Using a user-supplied profiles.yml file. If using this mode, set path_to_profiles_yml to the
    path to the file.
    2. Using cosmos to map Airflow connections to dbt profiles. If using this mode, set
    profile_mapping to a subclass of BaseProfileMapping.

    :param profile_name: The name of the dbt profile to use.
    :param target_name: The name of the dbt target to use.
    :param path_to_profiles_yml: The path to a profiles.yml file to use.
    :param profile_mapping: A mapping of Airflow connections to dbt profiles.
    """

    # should always be set to be explicit
    profile_name: str
    target_name: str

    # should be set if using a user-supplied profiles.yml
    path_to_profiles_yml: str | Path | None = None

    # should be set if using cosmos to map Airflow connections to dbt profiles
    profile_mapping: BaseProfileMapping | None = None

    def __post_init__(self) -> None:
        "Validates that we have enough information to render a profile."
        # if using a user-supplied profiles.yml, validate that it exists
        if self.path_to_profiles_yml:
            profiles_path = Path(self.path_to_profiles_yml)
            if not profiles_path.exists():
                raise CosmosValueError(f"Could not find profiles.yml at {self.path_to_profiles_yml}")

    def validate_profile(self) -> None:
        "Validates that we have enough information to render a profile."
        if not self.path_to_profiles_yml and not self.profile_mapping:
            raise CosmosValueError("Either path_to_profiles_yml or profile_mapping must be set to render a profile")

    @contextlib.contextmanager
    def ensure_profile(self, desired_profile_path: Path | None = None) -> Iterator[tuple[Path, dict[str, str]]]:
        "Context manager to ensure that there is a profile. If not, create one."
        if self.path_to_profiles_yml:
            logger.info("Using user-supplied profiles.yml at %s", self.path_to_profiles_yml)
            yield Path(self.path_to_profiles_yml), {}

        elif self.profile_mapping:
            profile_contents = self.profile_mapping.get_profile_file_contents(
                profile_name=self.profile_name, target_name=self.target_name
            )

            if desired_profile_path:
                logger.info(
                    "Writing profile to %s with the following contents:\n%s",
                    desired_profile_path,
                    profile_contents,
                )
                # write profile_contents to desired_profile_path using yaml library
                desired_profile_path.write_text(profile_contents)
                yield desired_profile_path, self.profile_mapping.env_vars
            else:
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_file = Path(temp_dir) / "profiles.yml"
                    logger.info(
                        "Creating temporary profiles.yml at %s with the following contents:\n%s",
                        temp_file,
                        profile_contents,
                    )
                    temp_file.write_text(profile_contents)
                    yield temp_file, self.profile_mapping.env_vars


@dataclass
class ExecutionConfig:
    """
    Contains configuration about how to execute dbt.

    :param execution_mode: The execution mode for dbt. Defaults to local
    :param dbt_executable_path: The path to the dbt executable. Defaults to dbt-ol or dbt if
    available on the path.
    """

    execution_mode: ExecutionMode = ExecutionMode.LOCAL
    dbt_executable_path: str | Path = shutil.which("dbt-ol") or shutil.which("dbt") or "dbt"
