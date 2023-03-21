:py:mod:`cosmos.providers.dbt.render`
=====================================

.. py:module:: cosmos.providers.dbt.render

.. autoapi-nested-parse::

   This module contains a function to render a dbt project into Cosmos entities.



Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.render.calculate_operator_class
   cosmos.providers.dbt.render.render_project



Attributes
~~~~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.render.logger


.. py:data:: logger



.. py:function:: calculate_operator_class(execution_mode: str, dbt_class: str) -> str


.. py:function:: render_project(dbt_project_name: str, dbt_root_path: str = '/usr/local/airflow/dbt', dbt_models_dir: str = 'models', task_args: Dict[str, Any] = {}, test_behavior: Literal[none, after_each, after_all] = 'after_each', emit_datasets: bool = True, conn_id: str = 'default_conn_id', select: Dict[str, List[str]] = {}, exclude: Dict[str, List[str]] = {}, execution_mode: Literal[local, docker, kubernetes] = 'local') -> cosmos.core.graph.entities.Group

   Turn a dbt project into a Group

   :param dbt_project_name: The name of the dbt project
   :param dbt_root_path: The root path to your dbt folder. Defaults to /usr/local/airflow/dbt
   :param task_args: Arguments to pass to the underlying dbt operators
   :param test_behavior: The behavior for running tests. Options are "none", "after_each", and "after_all".
       Defaults to "after_each"
   :param emit_datasets: If enabled test nodes emit Airflow Datasets for downstream cross-DAG dependencies
   :param conn_id: The Airflow connection ID to use in Airflow Datasets
   :param select: A dict of dbt selector arguments (i.e., {"tags": ["tag_1", "tag_2"]})
   :param exclude: A dict of dbt exclude arguments (i.e., {"tags": ["tag_1", "tag_2]}})
   :param execution_mode: The execution mode in which the dbt project should be run.
       Options are "local", "docker", and "kubernetes".
       Defaults to "local"
