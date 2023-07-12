from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DbtProject:
    name: str
    root_dir: Path
    models_dir: Path | None = None
    seeds_dir: Path | None = None
    snapshots_dir: Path | None = None
    manifest: Path | None = None

    def __post_init__(self):
        if self.models_dir is None:
            self.models_dir = self.root_dir / "models"
        if self.seeds_dir is None:
            self.seeds_dir = self.root_dir / "seeds"
        if self.snapshots_dir is None:
            self.snapshots_dir = self.root_dir / "snapshots"

    @property
    def dir(self) -> Path:
        return self.root_dir / self.name
