.. _operator-args:

Operator arguments
==================

It is possible to pass arguments to Cosmos operators in two ways. Either by passing them when directly instantiating Cosmos Operators
or by defining the ``operator_args`` within a ``DbtDag`` or a ``DbtTaskGroup`` instance.
The value of ``operator_args`` should be a dictionary that will become the underlining operators' ``kwargs``.

Example of how to set Kubernetes-specific operator arguments:

.. code-block:: python

    DbtDag(
        # ...
        operator_args={
            "queue": "kubernetes",
            "image": "dbt-jaffle-shop:1.0.0",
            "image_pull_policy": "Always",
            "get_logs": True,
            "is_delete_operator_pod": False,
            "namespace": "default",
        },
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.KUBERNETES,
        ),
    )

Example of setting a Cosmos-specific operator argument:

.. code-block:: python

    DbtDag(
        # ...
        operator_args={"dbt_cmd_global_flags": ["--cache-selected-only"]}
    )



Summary of Cosmos-specific arguments
------------------------------------

dbt-related
...........

- ``append_env``: Expose the operating system environment variables to the ``dbt`` command. The default is ``False``.
- ``dbt_cmd_flags``: List of command flags to pass to ``dbt`` command, added after dbt subcommand
- ``dbt_cmd_global_flags``: List of ``dbt`` `global flags <https://docs.getdbt.com/reference/global-configs/about-global-configs>`_ to be passed to the ``dbt`` command, before the subcommand
- ``dbt_executable_path``: Path to dbt executable.
- ``env``: Declare, using a Python dictionary, values to be set as environment variables when running ``dbt`` commands.
- ``fail_fast``: ``dbt`` exits immediately if ``dbt`` fails to process a resource.
- ``models``: Specifies which nodes to include.
- ``no_version_check``: If set, skip ensuring ``dbt``'s version matches the one specified in the ``dbt_project.yml``.
- ``quiet``: run ``dbt`` in silent mode, only displaying its error logs.
- ``vars``: Supply variables to the project. This argument overrides variables defined in the ``dbt_project.yml``.
- ``warn_error``: convert ``dbt`` warnings into errors.

Airflow-related
...............

- ``cancel_query_on_kill``: If true, cancel any running queries when the task's ``on_kill()`` is executed.
- ``output_encoding``: Declare the character encoding to parse the ``dbt`` command output.
- ``skip_exit_code``: If the task exits with this exit code, leave the task in ``skipped`` state (default: 99).

Sample usage
............

.. code-block:: python

    DbtTaskGroup(
        # ...
        operator_args={
            "append_env": True,
            "dbt_cmd_flags": ["--models", "stg_customers"],
            "dbt_cmd_global_flags": ["--cache-selected-only"],
            "dbt_executable_path": Path("/home/user/dbt"),
            "env": {"MY_ENVVAR": "some-value"},
            "fail_fast": True,
            "no_version_check": True,
            "quiet": True,
            "vars": {
                "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
                "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
            },
            "warn_error": True,
            "cancel_query_on_kill": False,
            "output_enconding": "utf-8",
            "skip_exit_code": 1,
        }
    )
