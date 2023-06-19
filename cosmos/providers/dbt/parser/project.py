"""
Used to parse and extract information from dbt projects.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path


from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ParsedNode


logger = logging.getLogger(__name__)


@dataclass
class DbtModel:
    """
    Represents a single dbt model. (This class also hold snapshots)
    """

    unique_id: str
    resource_type: str

    def __post_init__(self) -> None:
        self.node: ParsedNode | None = None
        self.child_models = []
        self.child_tests = []
        self.task = None

    @property
    def name(self):
        try:
            return self.node.identifier
        except AttributeError:
            return None

    def __repr__(self) -> str:
        """
        Returns the string representation of the model.
        """
        return f"DbtModel(unique_id='{self.unique_id}', resource_type='{self.resource_type}', name='{self.name}')"


@dataclass
class DbtProject:
    """
    Represents a single dbt project.
    """

    # required, user-specified instance variables
    project_name: str

    # optional, user-specified instance variables
    dbt_root_path: str = "/usr/local/airflow/dags/dbt"

    # private instance variables for managing state
    project_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        """
        Initializes the parser.
        """
        # set the project and model dirs
        self.project_dir = Path(os.path.join(self.dbt_root_path, self.project_name))

        runner = dbtRunner()
        res: dbtRunnerResult = runner.invoke(["parse"])

        manifest: Manifest = res.result
        self.manifest = manifest
        if manifest is None:
            raise Exception("Manifest is None", self.project_dir.as_posix(), res)
        logger.info("Runner", runner)
        logger.info("manifest", manifest)
