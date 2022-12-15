from airflow.models import DAG

from cosmos.core.render import CosmosTaskGroup
from cosmos.providers.dbt.parser.project import DbtProjectParser


def DbtTaskGroup(
    dbt_project_name: str,
    conn_id: str,
    dag: DAG,
    dbt_args: dict = None,
    emit_datasets: bool = True,
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
    :param emit_datasets: If enabled test nodes emit Airflow Datasets for downstream cross-DAG dependencies
    :type emit_datasets: bool
    :param kwargs: Additional kwargs to pass to the DAG
    :type kwargs: dict
    :return: The rendered Task Group
    :rtype: airflow.utils.task_group.TaskGroup
    """
    # first, parse the dbt project and get a Group
    parser = DbtProjectParser(
        project_name=dbt_project_name, conn_id=conn_id, dbt_args=dbt_args, emit_datasets=emit_datasets
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
