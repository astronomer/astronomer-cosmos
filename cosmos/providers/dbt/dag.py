from cosmos.core.render.dag import CosmosDag
from cosmos.providers.dbt.parser.project import DbtProjectParser


class DbtDag(CosmosDag):
    """
    Render a dbt project as an Airflow DAG.
    """

    def __init__(self, project_name: str, **kwargs):
        """
        :param project_dir: The path to the dbt project directory
        :type project_dir: str
        :param kwargs: Additional arguments to pass to the DAG constructor
        :type kwargs: dict

        :return: The rendered DAG
        :rtype: airflow.models.DAG
        """
        self.project_name = project_name
        self.kwargs = kwargs

    def render(self):
        """
        Render the DAG.

        :return: The rendered DAG
        :rtype: airflow.models.DAG
        """
        # first, parse the dbt project and get a Group
        parser = DbtProjectParser(
            project_name=self.project_name,
        )
        group = parser.parse()

        # then, render the Group as a DAG
        dag = super().render(group)

        # finally, update the DAG with any additional kwargs
        for key, value in self.kwargs.items():
            setattr(dag, key, value)

        return dag
