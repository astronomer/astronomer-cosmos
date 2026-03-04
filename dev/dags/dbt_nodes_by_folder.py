"""
Build dbt_nodes_by_folder from a dbt project path (seeds + models by folder).

Use as a library:
  from dbt_nodes_by_folder import build_dbt_nodes_by_folder
  d = build_dbt_nodes_by_folder(Path("/path/to/dbt_project"))

Use as a standalone script for ad-hoc dbt commands:
  python dbt_nodes_by_folder.py /path/to/dbt_project
  python dbt_nodes_by_folder.py   # uses current directory

Output is JSON suitable for dbt --vars, e.g.:
  dbt run --vars "$(python dbt_nodes_by_folder.py .)"
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

MODEL_EXTENSIONS = (".sql", ".py")
SEED_EXTENSIONS = (".csv", ".parquet", ".xlsx", ".xls")


def build_dbt_nodes_by_folder(project_path: Path | str) -> dict[str, list[str]]:
    """
    Build dbt_nodes_by_folder: keys like "seeds", "models/staging", "models/production";
    values are sorted lists of node names (file stems).

    :param project_path: Path to the dbt project root (directory containing dbt_project.yml).
    :return: Dict mapping folder keys to lists of node names.
    """
    project_path = Path(project_path).resolve()
    dbt_nodes_by_folder: dict[str, list[str]] = {}

    seeds_dir = project_path / "seeds"
    if seeds_dir.is_dir():
        names = [f.stem for f in seeds_dir.iterdir() if f.is_file() and f.suffix.lower() in SEED_EXTENSIONS]
        if names:
            dbt_nodes_by_folder["seeds"] = sorted(names)

    models_dir = project_path / "models"
    if models_dir.is_dir():
        for subdir in sorted(models_dir.iterdir()):
            if not subdir.is_dir():
                continue
            names = [f.stem for f in subdir.iterdir() if f.is_file() and f.suffix.lower() in MODEL_EXTENSIONS]
            if names:
                dbt_nodes_by_folder["models/" + subdir.name] = sorted(names)

    return dbt_nodes_by_folder


def to_vars_json(dbt_nodes_by_folder: dict[str, list[str]]) -> str:
    """Format as JSON for dbt --vars (single line)."""
    return json.dumps({"dbt_nodes_by_folder": dbt_nodes_by_folder}, separators=(",", ":"))


def main() -> int:
    project_path = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path.cwd()
    if not project_path.is_dir():
        print(f"Error: not a directory: {project_path}", file=sys.stderr)
        return 1
    dbt_nodes_by_folder = build_dbt_nodes_by_folder(project_path)
    print(to_vars_json(dbt_nodes_by_folder))
    return 0


if __name__ == "__main__":
    sys.exit(main())
