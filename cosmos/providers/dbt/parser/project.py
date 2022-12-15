import logging
import os

from airflow.datasets import Dataset

from cosmos.core.graph.entities import Group, Task
from cosmos.core.parse.base_parser import BaseParser

from .jinja import extract_deps_from_models
from .utils import validate_directory

logger = logging.getLogger(__name__)


class DbtProjectParser(BaseParser):
    """
    Parses a dbt project into `cosmos` entities.
    """

    def __init__(
        self,
        project_name: str,
        conn_id: str,
        dbt_args: dict = {},
        dbt_root_path: str = "/usr/local/airflow/dbt",
        emit_datasets: bool = True,
    ):
        """
        Initializes the parser.

        :param project_name: The path to the dbt project, relative to the dbt root path
        :type project_path: str
        :param conn_id: The Airflow connection ID to use for the dbt profile
        :type conn_id: str
        :param dbt_args: Parameters to pass to the underlying dbt operators
        :type dbt_args: dict
        :param dbt_root_path: The path to the dbt root directory
        :type dbt_root_path: str
        :param emit_datasets: If enabled test nodes emit Airflow Datasets for downstream cross-DAG dependencies
        :type emit_datasets: bool
        """
        # validate the dbt root path
        self.project_name = project_name
        validate_directory(dbt_root_path, "dbt_root_path")
        self.dbt_root_path = dbt_root_path

        # validate the project path
        project_path = os.path.join(dbt_root_path, project_name)
        validate_directory(project_path, "project_path")
        self.project_path = project_path

        # validate the conn_id
        if not conn_id:
            raise ValueError("conn_id must be provided")
        self.conn_id = conn_id

        self.dbt_args = dbt_args or {}

        # emit datasets from test tasks unless false
        self.emit_datasets = emit_datasets

    def parse(self):
        """
        Parses the dbt project in the project_path into `cosmos` entities.
        """
        models = extract_deps_from_models(project_path=self.project_path)

        project_name = os.path.basename(self.project_path)

        base_group = Group(id=project_name)

        entities = {}

        for model, deps in models.items():
            args = {
                "conn_id": self.conn_id,
                "project_dir": self.project_path,
                "models": model,
                **self.dbt_args,
            }
            # make the run task
            run_task = Task(
                id=f"{model}_run",
                operator_class="cosmos.providers.dbt.core.operators.DBTRunOperator",
                arguments=args,
            )
            entities[run_task.id] = run_task

            # make the test task
            if self.emit_datasets:
                args["outlets"] = [Dataset(f"DBT://{self.conn_id.upper()}/{self.project_name.upper()}/{model.upper()}")]
            test_task = Task(
                id=f"{model}_test",
                operator_class="cosmos.providers.dbt.core.operators.DBTTestOperator",
                upstream_entity_ids=[run_task.id],
                arguments=args,
            )
            entities[test_task.id] = test_task

            # make the group
            model_group = Group(
                id=model,
                entities=[run_task, test_task],
            )
            entities[model_group.id] = model_group

            # just add to base group for now
            base_group.add_entity(entity=model_group)

        # add dependencies
        for model, deps in models.items():
            for dep in deps:
                try:
                    dep_task = entities[dep]
                    entities[model].add_upstream(dep_task)
                except KeyError:
                    logger.error(f"Dependency {dep} not found for model {model}")

        return base_group
