Execution Config
==================

Cosmos aims to give you control over how your dbt project is executed when running in airflow.
It does this by exposing a ``cosmos.config.ExecutionConfig`` class that you can use to configure how your DAGs are executed.

The ``ExecutionConfig`` class takes the following arguments:

- ``execution_mode``: The way dbt is run when executing within airflow. For more information, see the `execution modes <../getting_started/execution-modes.html>`_ page.
- ``invocation_mode`` (new in v1.4): The way dbt is invoked within the execution mode. This is only configurable for ``ExecutionMode.LOCAL``. For more information, see `invocation modes <../getting_started/execution-modes.html#invocation-modes>`_.
- ``test_indirect_selection``: The mode to configure the test behavior when performing indirect selection.
- ``dbt_executable_path``: The path to the dbt executable for dag generation. Defaults to dbt if available on the path.
- ``dbt_project_path``: Configures the dbt project location accessible at runtime for dag execution. This is the project path in a docker container for ``ExecutionMode.DOCKER`` or ``ExecutionMode.KUBERNETES``. Mutually exclusive with ``ProjectConfig.dbt_project_path``.
- ``virtualenv_dir`` (new in v1.6): Directory path to locate the (cached) virtual env that should be used for execution when execution mode is set to ``ExecutionMode.VIRTUALENV``.
