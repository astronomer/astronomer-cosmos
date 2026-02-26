#!/usr/bin/env python3
"""
Inspect a dbt project's models folder and print folder_models as YAML for dbt --vars.
Output is valid YAML flow style (works when shell strips double quotes). One-liner:
  python scripts/generate_folder_vars.py . | xargs -I {} dbt run --vars '{}'

Usage:
  python scripts/generate_folder_vars.py /path/to/dbt_project
"""

from __future__ import annotations

import sys
from pathlib import Path

MODEL_EXTENSIONS = (".sql", ".py")


def get_models_dir(project_path: Path) -> Path:
    """Return the models directory (project_path/models)."""
    project_path = Path(project_path).resolve()
    models_dir = project_path / "models"
    if not models_dir.is_dir():
        raise FileNotFoundError(f"No models directory at {models_dir}")
    return models_dir


def build_folder_models(project_path: Path) -> dict[str, list[str]]:
    """Inspect models/ subdirs and return folder name -> list of model names (from .sql/.py files)."""
    models_dir = get_models_dir(project_path)
    result: dict[str, list[str]] = {}
    for subdir in sorted(models_dir.iterdir()):
        if not subdir.is_dir():
            continue
        names = [f.stem for f in subdir.iterdir() if f.is_file() and f.suffix.lower() in MODEL_EXTENSIONS]
        if names:
            result[subdir.name] = sorted(names)
    return result


def to_vars_yaml(folder_models: dict[str, list[str]]) -> str:
    """Format as single-line YAML flow style with spaces after colons (valid for dbt --vars)."""
    inner = ", ".join(f"{k}: [{', '.join(v)}]" for k, v in sorted(folder_models.items()))
    return f"{{folder_models: {{{inner}}}}}"


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python generate_folder_vars.py <path_to_dbt_project>", file=sys.stderr)
        return 1
    project_path = Path(sys.argv[1]).resolve()
    try:
        folder_models = build_folder_models(project_path)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        return 1
    print(to_vars_yaml(folder_models))
    return 0


if __name__ == "__main__":
    sys.exit(main())
