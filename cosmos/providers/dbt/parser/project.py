import json
import os

from cosmos.core.graph.group import Group
from cosmos.core.graph.task import Task
from cosmos.core.parse.base_parser import BaseParser

from .utils import validate_directory
from .jinja import extract_deps_from_models


class DbtProjectParser(BaseParser):
    """
    Parses a dbt project into `cosmos` entities.
    """

    def __init__(
        self,
        project_name: str,
        dbt_root_path: str = "/usr/local/airflow/dags/dbt",
    ):
        """
        Initializes the parser.

        :param project_name: The path to the dbt project, relative to the dbt root path
        :type project_path: str
        :param dbt_root_path: The path to the dbt root directory
        :type dbt_root_path: str
        """
        # validate the dbt root path
        validate_directory(dbt_root_path, "dbt_root_path")
        self.dbt_root_path = dbt_root_path

        # validate the project path
        project_path = os.path.join(dbt_root_path, project_name)
        validate_directory(project_path, "project_path")
        self.project_path = project_path
            

    def parse(self):
        """
        Parses the dbt project in the project_path into `cosmos` entities.
        """
        models = extract_deps_from_models(project_path=self.project_path)
        
        base_group = Group(group_id="dbt_project")

        for model, deps in models.items():
            # make the run task
            run_task = Task(
                task_id=f"{model}",
                operator_class="airflow.operators.empty.EmptyOperator",
                arguments={
                    "model_name": model,
                    "project_path": self.project_path,
                    "dbt_root_path": self.dbt_root_path,
                },
            )

            # make the test task
            test_task = Task(
                task_id=f"{model}_test",
                operator_class="airflow.operators.empty.EmptyOperator",
                upstream_task_ids=[model],
                arguments={
                    "model_name": model,
                    "project_path": self.project_path,
                    "dbt_root_path": self.dbt_root_path,
                },
            )

            # make the group
            # group = Group(
            #     group_id=model,
            #     tasks=[run_task, test_task],
            # )

            # just add to base group for now
            base_group.add_task(task=run_task)
            base_group.add_task(task=test_task)

        print(base_group)
        return base_group
