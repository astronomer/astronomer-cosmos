.. _execution-config:

Execution Config
================

Cosmos aims to give you control over how your dbt project is executed when running in airflow.
It does this by exposing a ``cosmos.config.ExecutionConfig`` class that you can use to configure how your DAGs are executed.

The ``ExecutionConfig`` class takes the following arguments:

- ``execution_mode``: The way dbt is run when executing within airflow. For more information, see the :ref:`execution-modes` page.
- ``invocation_mode`` (new in v1.4): The way dbt is invoked within the execution mode. This is only configurable for ``ExecutionMode.LOCAL``. For more information, see :ref:`invocation-mode`.
- ``test_indirect_selection``: The mode to configure the test behavior when performing indirect selection.
- ``dbt_executable_path``: The path to the dbt executable for dag generation. Defaults to dbt if available on the path.
- ``dbt_project_path``: Configures the dbt project location accessible at runtime for dag execution. This is the project path in a Docker container for ``ExecutionMode.DOCKER`` or ``ExecutionMode.KUBERNETES``. Mutually exclusive with ``ProjectConfig.dbt_project_path``.
- ``virtualenv_dir`` (new in v1.6): Directory path to locate the (cached) virtual env that should be used for execution when execution mode is set to ``ExecutionMode.VIRTUALENV``.
- ``async_py_requirements`` (new in v1.9): A list of Python packages to install when ``ExecutionMode.AIRFLOW_ASYNC`` is used. This parameter is required only when ``enable_setup_async_task`` and ``enable_teardown_async_task`` are set to ``True``. Example value: ``["dbt-postgres==1.9.0"]``.
- ``setup_operator_args`` (new in v1.12): A dictionary of `Apache Airflow® <https://airflow.apache.org/>`_ operator arguments to be passed to DbtProducerWatcherOperator when using ExecutionMode.WATCHER. These values override any arguments provided through operator_args for the producer task.
- ``install_dbt_deps`` (new in v1.12): Whether to run ``dbt deps`` at task execution time. When set, it overrides ``ProjectConfig.install_dbt_deps`` for execution only (it does not affect DAG parsing). Accepts a boolean or an `Airflow Jinja-templated <https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#templates-reference>`_ string (e.g. ``"{{ params.install_deps }}"``) that resolves to a boolean at task execution time, so ``dbt deps`` can be controlled per DAG run. Only supported for ``ExecutionMode.LOCAL``, ``ExecutionMode.VIRTUALENV`` and ``ExecutionMode.WATCHER``.

Control dbt deps per DAG run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ExecutionConfig.install_dbt_deps`` can be templated, which is useful when you want to decide whether to
run ``dbt deps`` at task execution time — for example, only on the first run or when package dependencies
change — driven by an Airflow ``Param``. Because Airflow templates are rendered only at task execution time,
this overrides ``ProjectConfig.install_dbt_deps`` for execution only and does not affect DAG parsing.

.. code-block:: python

    from airflow.models.param import Param
    from cosmos import DbtDag, ExecutionConfig, ProjectConfig

    dag = DbtDag(
        # ...
        project_config=ProjectConfig(dbt_project_path="/path/to/dbt/project"),
        execution_config=ExecutionConfig(install_dbt_deps="{{ params.install_deps }}"),
        params={
            "install_deps": Param(
                False, type="boolean", description="Install dbt dependencies?"
            ),
        },
        render_template_as_native_obj=True,
    )

Setting ``render_template_as_native_obj=True`` is recommended so the rendered value is a real boolean.
Without it Airflow renders the template to a string (e.g. ``"False"``); Cosmos normalizes both forms to
a boolean when deciding whether to run ``dbt deps``.
