#!/usr/bin/env python3
"""
Inspect a dbt project's models and seeds and print dbt_nodes_by_folder as YAML for dbt --vars.
Keys are full paths: seeds, models/staging, models/production. One-liner:
  python scripts/generate_folder_vars.py . | xargs -I {} dbt run --vars '{}'

Usage:
  python scripts/generate_folder_vars.py /path/to/dbt_project
"""

from __future__ import annotations

import sys
from pathlib import Path

MODEL_EXTENSIONS = (".sql", ".py")
SEED_EXTENSIONS = (".csv", ".parquet", ".xlsx", ".xls")


def get_models_dir(project_path: Path) -> Path:
    """Return the models directory (project_path/models)."""
    project_path = Path(project_path).resolve()
    models_dir = project_path / "models"
    if not models_dir.is_dir():
        raise FileNotFoundError(f"No models directory at {models_dir}")
    return models_dir


def get_seeds_dir(project_path: Path) -> Path:
    """Return the seeds directory (project_path/seeds)."""
    project_path = Path(project_path).resolve()
    return project_path / "seeds"


def build_dbt_nodes_by_folder(project_path: Path) -> dict[str, list[str]]:
    """Build dbt_nodes_by_folder: keys seeds, models/<subdir>; values list of node names."""
    result: dict[str, list[str]] = {}

    seeds_dir = get_seeds_dir(project_path)
    if seeds_dir.is_dir():
        names = [f.stem for f in seeds_dir.iterdir() if f.is_file() and f.suffix.lower() in SEED_EXTENSIONS]
        if names:
            result["seeds"] = sorted(names)

    models_dir = get_models_dir(project_path)
    for subdir in sorted(models_dir.iterdir()):
        if not subdir.is_dir():
            continue
        names = [f.stem for f in subdir.iterdir() if f.is_file() and f.suffix.lower() in MODEL_EXTENSIONS]
        if names:
            result["models/" + subdir.name] = sorted(names)

    return result


def inject_folder_models_dict(context, operator) -> dict[str, dict[str, list[str]]]:
    """Inject dbt_nodes_by_folder into operator.vars for runtime."""
    operator.vars["dbt_nodes_by_folder"] = build_dbt_nodes_by_folder(operator.project_path)


def to_vars_yaml(dbt_nodes_by_folder: dict[str, list[str]]) -> str:
    """Format as single-line YAML flow style (valid for dbt --vars)."""
    inner = ", ".join(f"{k}: [{', '.join(v)}]" for k, v in sorted(dbt_nodes_by_folder.items()))
    return f"{{dbt_nodes_by_folder: {{{inner}}}}}"


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python generate_folder_vars.py <path_to_dbt_project>", file=sys.stderr)
        return 1
    project_path = Path(sys.argv[1]).resolve()
    try:
        dbt_nodes_by_folder = build_dbt_nodes_by_folder(project_path)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        return 1
    print(to_vars_yaml(dbt_nodes_by_folder))
    return 0


if __name__ == "__main__":
    sys.exit(main())
