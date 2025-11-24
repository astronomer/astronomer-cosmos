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
- ``async_py_requirements`` (new in v1.9): A list of Python packages to install when ``ExecutionMode.AIRFLOW_ASYNC`` is used. This parameter is required only when ``enable_setup_async_task`` and ``enable_teardown_async_task`` are set to ``True``. Example value: ``["dbt-postgres==1.9.0"]``.
- ``setup_operator_args`` (new in v1.12): A dictionary of Airflow operator arguments to be passed to DbtProducerWatcherOperator when using ExecutionMode.WATCHER. These values override any arguments provided through operator_args for the producer task.
