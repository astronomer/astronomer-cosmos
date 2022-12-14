import json
import logging
import os

from cosmos.core.graph.group import Group
from cosmos.core.graph.task import Task
from cosmos.core.parse.base_parser import BaseParser
from cosmos.providers.dbt.core.utils.profiles_generator import create_default_profiles, map_profile

from .utils import validate_directory

logger = logging.getLogger(__name__)


class DbtProjectParser(BaseParser):
    """
    Parses a dbt project into `cosmos` entities.
    """

    def __init__(
        self,
        project_path: str,
        conn_id: str,
        dbt_root_path: str = "/usr/local/airflow/dags/dbt",
        dbt_profiles_dir: str = "/usr/local/airflow/.dbt",
    ):
        """
        Initializes the parser.

        :param project_path: The path to the dbt project, relative to the dbt root path
        :type project_path: str
        :param conn_id: The Airflow connection ID to use for the dbt run
        :type conn_id: str
        :param dbt_root_path: The path to the dbt root directory
        :type dbt_root_path: Optional[str]
        :param dbt_profiles_dir: The path to the dbt profiles directory
        :type dbt_profiles_dir: Optional[str]
        """
        self.conn_id = conn_id

        # validate the dbt root path
        validate_directory(dbt_root_path, "dbt_root_path")
        self.dbt_root_path = dbt_root_path

        # create and validate the project path
        project_path = os.path.join(dbt_root_path, project_path)
        validate_directory(project_path, "project_path")
        self.project_path = project_path

        # validate the dbt profiles directory
        try:
            validate_directory(dbt_profiles_dir, "dbt_profiles_dir")
        except ValueError:
            # if the directory doesn't exist, create it
            os.mkdir(dbt_profiles_dir)

        self.dbt_profiles_dir = dbt_profiles_dir

    def parse(self) -> Group:
        """
        Parses the dbt project in the project_path into `cosmos` entities.
        """
        # ensure the manifest exists
        manifest = self.ensure_manifest()
        nodes = manifest["nodes"]

        base_group = Group(group_id="dbt_project")

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

        return base_group

    def ensure_manifest(self):
        """
        Ensures that the dbt project has a manifest.json file.
        """
        manifest_path = os.path.join(self.project_path, "target/manifest.json")

        # if the manifest doesn't exist, we need to run dbt list
        if not os.path.exists(manifest_path):
            create_default_profiles()
            profile, _ = map_profile(conn_id=self.conn_id)

            # run dbt compile
            logger.info("Running dbt list to generate manifest.json")
            logger.info(
                os.popen(
                    f"""
                dbt list \
                --profiles-dir {self.dbt_profiles_dir} \
                --project-dir {self.project_path} \
                --profile {profile}
                """
                )
            )
        else:
            logger.info("Using existing manifest.json")

        # read the manifest
        with open(manifest_path, encoding="utf-8") as manifest_contents:
            manifest = json.load(manifest_contents)

        return manifest
