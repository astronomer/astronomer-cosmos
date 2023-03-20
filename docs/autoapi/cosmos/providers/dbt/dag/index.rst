:py:mod:`cosmos.providers.dbt.dag`
==================================

.. py:module:: cosmos.providers.dbt.dag

.. autoapi-nested-parse::

   This module contains a function to render a dbt project as an Airflow DAG.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   cosmos.providers.dbt.dag.DbtDag




.. py:class:: DbtDag(dbt_project_name: str, conn_id: str, dbt_args: Dict[str, Any] = {}, emit_datasets: bool = True, dbt_root_path: str = '/usr/local/airflow/dbt', dbt_models_dir: str = 'models', test_behavior: Literal[none, after_each, after_all] = 'after_each', select: Dict[str, List[str]] = {}, exclude: Dict[str, List[str]] = {}, execution_mode: Literal[local, docker, kubernetes] = 'local', *args: Any, **kwargs: Any)

   Bases: :py:obj:`cosmos.core.airflow.CosmosDag`

   Render a dbt project as an Airflow DAG. Overrides the Airflow DAG model to allow
   for additional configs to be passed.

   :param dbt_project_name: The name of the dbt project
   :param dbt_root_path: The path to the dbt root directory
   :param dbt_models_dir: The path to the dbt models directory within the project
   :param conn_id: The Airflow connection ID to use for the dbt profile
   :param dbt_args: Parameters to pass to the underlying dbt operators, can include dbt_executable_path to utilize venv
   :param emit_datasets: If enabled test nodes emit Airflow Datasets for downstream cross-DAG dependencies
   :param test_behavior: The behavior for running tests. Options are "none", "after_each", and "after_all".
       Defaults to "after_each"
   :param select: A dict of dbt selector arguments (i.e., {"tags": ["tag_1", "tag_2"]})
   :param exclude: A dict of dbt exclude arguments (i.e., {"tags": ["tag_1", "tag_2"]})
   :param execution_mode: The execution mode in which the dbt project should be run.
       Options are "local", "docker", and "kubernetes".
       Defaults to "local"
