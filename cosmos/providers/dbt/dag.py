from cosmos.core.render import CosmosDag
from cosmos.providers.dbt.parser.project import DbtProjectParser


def DbtDag(
    dbt_project_name: str,
    conn_id: str,
    dbt_args: dict = None,
    emit_datasets: bool = True,
    **kwargs,
):
    """
    Render a dbt project as an Airflow DAG.

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
    :return: The rendered DAG
    :rtype: airflow.models.DAG
    """
    # first, parse the dbt project and get a Group
    parser = DbtProjectParser(
        project_name=dbt_project_name, conn_id=conn_id, dbt_args=dbt_args, emit_datasets=emit_datasets
    )
    group = parser.parse()

    # then, render the Group as a DAG
    dag = CosmosDag(group=group, dag_args=kwargs).render()

    return dag
