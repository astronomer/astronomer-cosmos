from dataclasses import dataclass
from pathlib import Path


@dataclass
class DbtProject:
    name: str
    root_dir: Path
    models_dir: Path
    seeds_dir: Path
    snapshots_dir: Path

    @property
    def dir(self) -> Path:
        return self.root_dir / self.name
