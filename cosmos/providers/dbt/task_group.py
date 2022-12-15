from airflow.models import DAG

from cosmos.providers.dbt.parser.project import DbtProjectParser
from cosmos.core.render import CosmosTaskGroup


def DbtTaskGroup(
    dbt_project_name: str,
    conn_id: str,
    dag: DAG,
    dbt_args: dict = None,
    **kwargs,
):
    """
    Render a dbt project as an Airflow TaskGroup.

    :param dbt_project_name: The name of the dbt project
    :type dbt_project_name: str
    :param conn_id: The Airflow connection ID to use for the dbt profile
    :type conn_id: str
    :param dbt_args: Parameters to pass to the underlying dbt operators
    :type dbt_args: dict
    :param kwargs: Additional kwargs to pass to the DAG
    :type kwargs: dict
    :return: The rendered DAG
    :rtype: airflow.models.DAG
    """
    # first, parse the dbt project and get a Group
    parser = DbtProjectParser(
        project_name=dbt_project_name,
        conn_id=conn_id,
        dbt_args=dbt_args,
    )
    group = parser.parse()

    # then, render the Group as an Airflow TaskGroup
    task_group = CosmosTaskGroup(
        group=group,
        dag=dag,
        task_group=kwargs.get("task_group"),
        task_group_args=kwargs,
    ).render()

    return task_group