import json
import os

from core.graph.group import Group
from core.graph.task import Task
from core.parse.base_parser import BaseParser

from .utils import validate_directory


class DbtProjectParser(BaseParser):
    """
    Parses a dbt project into `cosmos` entities.
    """

    def __init__(
        self,
        project_path: str,
        dbt_root_path: str,
        dbt_profiles_dir: str,
    ):
        """
        Initializes the parser.

        :param project_path: The path to the dbt project, relative to the dbt root path
        :type project_path: str
        :param dbt_root_path: The path to the dbt root directory
        :type dbt_root_path: str
        :param dbt_profiles_dir: The path to the dbt profiles directory
        :type dbt_profiles_dir: str
        """
        # Validate the project path
        validate_directory(project_path, "project_path")
        self.project_path = project_path

        # Validate the dbt root path
        validate_directory(dbt_root_path, "dbt_root_path")
        self.dbt_root_path = dbt_root_path

        # Validate the dbt profiles directory
        validate_directory(dbt_profiles_dir, "dbt_profiles_dir")
        self.dbt_profiles_dir = dbt_profiles_dir

    def parse(self):
        """
        Parses the dbt project in the project_path into `cosmos` entities.
        """
        manifest = self.get_manifest()
        nodes = manifest["nodes"]

        for node_name, node in nodes.items():
            if node_name.split(".")[0] == "model":
                # make the run task
                run_task = Task(
                    task_id=node_name,
                    operator_class="cosmos.providers.dbt.operators.DbtRunModel",
                    arguments={
                        "model_name": node_name,
                        "project_path": self.project_path,
                        "dbt_root_path": self.dbt_root_path,
                        "dbt_profiles_dir": self.dbt_profiles_dir,
                    },
                )

                # make the test task
                test_task = Task(
                    task_id=f"{node_name}_test",
                    operator_class="cosmos.providers.dbt.operators.DbtTestModel",
                    upstream_task_ids=[node_name],
                    arguments={
                        "model_name": node_name,
                        "project_path": self.project_path,
                        "dbt_root_path": self.dbt_root_path,
                        "dbt_profiles_dir": self.dbt_profiles_dir,
                    },
                )

                # make the group
                group = Group(
                    group_id=node_name,
                    tasks=[run_task, test_task],
                )

                # do something with the group for now
                print(group)

    def get_manifest(self) -> dict:
        """
        Gets the dbt manifest for the project.

        :return: The dbt manifest
        :rtype: dict
        """
        manifest_path = os.path.join(
            self.dbt_root_path,
            self.project_path,
            "target/manifest.json",
        )

        with open(manifest_path) as f:
            manifest = json.load(f)

        return manifest
