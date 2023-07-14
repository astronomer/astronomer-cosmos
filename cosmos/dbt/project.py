from __future__ import annotations
import yaml
from dataclasses import dataclass
from pathlib import Path


DEFAULT_PROFILE_FILE_NAME = "profiles.yml"


@dataclass
class DbtProject:
    name: str
    root_dir: Path
    pipeline_dir: Path | None = None
    models_dir: Path | None = None
    seeds_dir: Path | None = None
    snapshots_dir: Path | None = None
    manifest_path: Path | None = None
    profile_path: Path | None = None
    _cosmos_created_profile_file: bool = False

    def __post_init__(self):
        if self.pipeline_dir is None:
            self.pipeline_dir = self.root_dir / self.name
        if self.models_dir is None:
            self.models_dir = self.pipeline_dir / "models"
        if self.seeds_dir is None:
            self.seeds_dir = self.pipeline_dir / "seeds"
        if self.snapshots_dir is None:
            self.snapshots_dir = self.pipeline_dir / "snapshots"
        if self.profile_path is None:
            self.update_profile_path()

    @property
    def dir(self) -> Path:
        return self.root_dir / self.name

    def update_profile_path(self):
        if not self.profile_path:
            self.profile_path = self.pipeline_dir / "profiles.yml"
        if not self.profile_path.exists():
            with open(self.profile_path, "w") as fp:
                profile_content = {
                    self.name: {"target": "dev", "outputs": {"dev": {"type": "sqlite", "database": "/tmp/dummy.db"}}}
                }
                yaml.dump(profile_content, fp)
