Execution Config
==================

Cosmos aims to give you control over how your dbt project is executed when running in airflow.
It does this by exposing a ``cosmos.config.ExecutionConfig`` class that you can use to configure how your DAGs are executed.

The ``ExecutionConfig`` class takes the following arguments:

- ``execution_mode``: The way dbt is run when executing within airflow. For more information, see the `execution modes <../getting_started/execution-modes.html>`_ page.
- ``test_indirect_selection``: The mode to configure the test behavior when performing indirect selection.
- ``dbt_executable_path``: The path to the dbt executable for dag generation. Defaults to dbt if available on the path.
- ``dbt_project_path``: Configures the DBT project location accessible on their airflow controller for DAG rendering - Required when using ``load_method=LoadMode.DBT_LS`` or ``load_method=LoadMode.CUSTOM``
