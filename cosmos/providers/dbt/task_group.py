from airflow import TaskGroup

from cosmos.core.render.task_group import CosmosTaskGroup
from cosmos.providers.dbt.parser.project import DbtProjectParser


class DbtTaskGroup(CosmosTaskGroup):
    """
    Render a dbt project as an Airflow Task Group.
    """

    def __init__(self, project_dir: str, conn_id: str, **kwargs):
        """
        :param project_dir: The path to the dbt project directory
        :type project_dir: str
        :param conn_id: The Airflow connection ID to use for the dbt run
        :type conn_id: str
        :param kwargs: Additional arguments to pass to the DAG constructor
        :type kwargs: dict

        :return: The rendered DAG
        :rtype: airflow.models.DAG
        """
        self.project_dir = project_dir
        self.conn_id = conn_id
        self.kwargs = kwargs

    def render(self) -> TaskGroup:
        """
        Render the DAG.

        :return: The rendered task group
        :rtype: airflow.models.TaskGroup
        """
        # first, parse the dbt project and get a Group
        parser = DbtProjectParser(
            project_path=self.project_dir,
            conn_id=self.conn_id,
        )
        group = parser.parse()

        # then, render the Group as a DAG
        task_group = super().render(group)

        # finally, update the DAG with any additional kwargs
        for key, value in self.kwargs.items():
            setattr(task_group, key, value)

        return task_group
