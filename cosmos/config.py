"""Module that contains all Cosmos config classes."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from logging import getLogger

from cosmos.constants import TestBehavior, ExecutionMode, LoadMode
from cosmos.exceptions import CosmosValueError

logger = getLogger(__name__)


@dataclass
class RenderConfig:
    """
    Class for setting general Cosmos config.

    :param emit_datasets: If enabled test nodes emit Airflow Datasets for downstream cross-DAG
    dependencies
    :param test_behavior: The behavior for running tests. Defaults to after each
    :param execution_mode: The execution mode for dbt. Defaults to local
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
    :param manifest_path: The path to the dbt manifest file. Defaults to None
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
    Class for setting profile config.

    :param profile_name: The name of the dbt profile to use.
    :param target_name: The name of the dbt target to use.
    :param conn_id: The Airflow connection ID to use.
    """

    # should always be set to be explicit
    profile_name: str
    target_name: str
    conn_id: str
    profile_args: dict[str, str] = field(default_factory=dict)


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
