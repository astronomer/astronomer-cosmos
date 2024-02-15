Execution Config
==================

Cosmos aims to give you control over how your dbt project is executed when running in airflow.
It does this by exposing a ``cosmos.config.ExecutionConfig`` class that you can use to configure how your DAGs are executed.

The ``ExecutionConfig`` class takes the following arguments:

- ``execution_mode``: The way dbt is run when executing within airflow. For more information, see the `execution modes <../getting_started/execution-modes.html>`_ page.
- ``test_indirect_selection``: The mode to configure the test behavior when performing indirect selection.
- ``dbt_executable_path``: The path to the dbt executable for dag generation. Defaults to dbt if available on the path.
- ``dbt_project_path``: Configures the dbt project location accessible at runtime for dag execution. This is the project path in a docker container for ``ExecutionMode.DOCKER`` or ``ExecutionMode.KUBERNETES``. Mutually exclusive with ``ProjectConfig.dbt_project_path``.
- ``partial_parse``: If True, then the operator will use the ``partial_parse.msgpack`` during execution if it exists. If False, then the flag will be explicitly set to turn off partial parsing. For ``ExecutionMode.LOCAL`` and ``ExecutionMode.VIRTUALENV``, the ``partial_parse.msgpack`` file will be copied into temporary directory that dbt executes out of.
