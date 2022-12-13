from core.render.dag import CosmosDag
from providers.dbt.parser.project import DbtProjectParser


class DbtDag(CosmosDag):
    """
    Render a dbt project as an Airflow DAG.
    """

    def __init__(self, project_dir: str, **kwargs):
        """
        :param project_dir: The path to the dbt project directory
        :type project_dir: str
        :param kwargs: Additional arguments to pass to the DAG constructor
        :type kwargs: dict

        :return: The rendered DAG
        :rtype: airflow.models.DAG
        """
        self.project_dir = project_dir
        self.kwargs = kwargs

        return self.render()

    def render(self):
        """
        Render the DAG.

        :return: The rendered DAG
        :rtype: airflow.models.DAG
        """
        # first, parse the dbt project and get a Group
        parser = DbtProjectParser(
            project_path=self.project_dir,
        )
        group = parser.parse()

        # then, render the Group as a DAG
        dag = super().render(group)

        # finally, update the DAG with any additional kwargs
        for key, value in self.kwargs.items():
            setattr(dag, key, value)

        return dag
