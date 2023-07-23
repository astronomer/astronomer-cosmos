"Contains the DbtNode class which is used to represent a dbt node (e.g. model, seed, snapshot)."
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from cosmos.constants import DbtResourceType


@dataclass
class DbtNode:
    """
    Metadata related to a dbt node (e.g. model, seed, snapshot).
    """

    name: str
    unique_id: str
    resource_type: DbtResourceType
    depends_on: list[str]
    file_path: Path
    tags: list[str] = field(default_factory=lambda: [])
    config: dict[str, Any] = field(default_factory=lambda: {})
