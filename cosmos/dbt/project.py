from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path


DEFAULT_PROFILE_FILE_NAME = "profiles.yml"


@dataclass
class DbtProject:
    name: str
    root_dir: Path
    models_dir: Path | None = None
    seeds_dir: Path | None = None
    snapshots_dir: Path | None = None
    manifest_path: Path | None = None
    profile_path: Path | None = None
    _cosmos_created_profile_file: bool = False

    def __post_init__(self) -> None:
        if self.models_dir is None:
            self.models_dir = self.dir / "models"
        if self.seeds_dir is None:
            self.seeds_dir = self.dir / "seeds"
        if self.snapshots_dir is None:
            self.snapshots_dir = self.dir / "snapshots"
        if self.profile_path is None:
            self.profile_path = self.dir / "profiles.yml"

    @property
    def dir(self) -> Path:
        """
        Path to dbt pipeline, defined by self.root_dir and self.name.
        """
        return self.root_dir / self.name

    def is_manifest_available(self) -> bool:
        """
        Check if the `dbt` project manifest is set and if the file exists.
        """
        return self.manifest_path is not None and Path(self.manifest_path).exists()

    def is_profile_yml_available(self) -> bool:
        """
        Check if the `dbt` profiles.yml file exists.
        """
        return Path(self.profile_path).exists() if self.profile_path else False
