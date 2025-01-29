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


Overriding operator arguments per dbt node (or group of nodes)
-------------------------------------------------------------

Cosmos 1.8 introduced the capability for users to customise the operator arguments per dbt node, or per group of dbt nodes.
This can be done by defining the arguments via a dbt meta property alongside other dbt project configurations.

Let's say there is a DbtTaskGroup that sets a default pool to run all the dbt tasks, but a user would like the model expensive
to run a separate pool.

Users could either use ``operator_args`` or ``default args`` for defining the default behavior:

.. code-block:: python

    dbt_task_group = DbtTaskGroup(
        # ...
        profile_config=ProfileConfig,
        default_args={"pool": "default_pool"},
    )

While configuring in the ``dbt_project.yml`` a different behaviour for the model "expensive", that should use the "expensive-pool":

.. code-block::

    version: 2
        models:
          - name: expensive
            description: description
            meta:
              cosmos:
                operator_kwargs:
                  pool: expensive-pool


More information about this feature can be found in `custom Airflow properties documentation <../getting_started/custom-airflow-properties.html>`_.

To learn how to customise the profile per dbt model or Cosmos task, check the `correspondent documentation <../profiles/index.html>`_.

Summary of Cosmos-specific arguments
------------------------------------

dbt-related
...........

- ``append_env``: Expose the operating system environment variables to the ``dbt`` command. For most execution modes, the default is ``False``, however, for execution modes ExecuteMode.LOCAL and ExecuteMode.VIRTUALENV, the default is True. This behavior is consistent with the LoadMode.DBT_LS command in forwarding the environment variables to the subprocess by default.
- ``dbt_cmd_flags``: List of command flags to pass to ``dbt`` command, added after dbt subcommand
- ``dbt_cmd_global_flags``: List of ``dbt`` `global flags <https://docs.getdbt.com/reference/global-configs/about-global-configs>`_ to be passed to the ``dbt`` command, before the subcommand
- ``dbt_executable_path``: Path to dbt executable.
- ``env``: (Deprecated since Cosmos 1.3 use ``ProjectConfig.env_vars`` instead) Declare, using a Python dictionary, values to be set as environment variables when running ``dbt`` commands.
- ``fail_fast``: ``dbt`` exits immediately if ``dbt`` fails to process a resource.
- ``models``: Specifies which nodes to include.
- ``no_version_check``: If set, skip ensuring ``dbt``'s version matches the one specified in the ``dbt_project.yml``.
- ``quiet``: run ``dbt`` in silent mode, only displaying its error logs.
- ``vars``: (Deprecated since Cosmos 1.3 use ``ProjectConfig.dbt_vars`` instead) Supply variables to the project. This argument overrides variables defined in the ``dbt_project.yml``.
- ``warn_error``: convert ``dbt`` warnings into errors.
- ``full_refresh``: If True, then full refresh the node. This only applies to model and seed nodes.
- ``install_deps``: When using ``ExecutionMode.LOCAL`` or ``ExecutionMode.VIRTUALENV``, run ``dbt deps`` every time a task is executed.

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


Template fields
---------------

Some of the operator args are `template fields <https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html#templating>`_ for your convenience.

These template fields can be useful for hooking into Airflow `Params <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html>`_, or for more advanced customization with `XComs <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html>`_.

The following operator args support templating, and are accessible both through the  ``DbtDag`` and ``DbtTaskGroup`` constructors in addition to being accessible standalone:

- ``env``
- ``vars``
- ``full_refresh`` (for the ``build``, ``seed``, and ``run`` operators since Cosmos 1.4.)

.. note::
    Using Jinja templating for ``env`` and ``vars`` may cause problems when using ``LoadMode.DBT_LS`` to render your DAG.

The following template fields are only selectable when using the operators in a standalone context (starting in Cosmos 1.4):

- ``select``
- ``exclude``
- ``selector``
- ``models``

Since Airflow resolves template fields during Airflow DAG execution and not DAG parsing,  the args above cannot be templated via ``DbtDag`` and ``DbtTaskGroup`` because both need to select dbt nodes during DAG parsing.

Additionally, the SQL for compiled dbt models is stored in the template fields, which is viewable in the Airflow UI for each task run.
This is provided for telemetry on task execution, and is not an operator arg.
For more information about this, see the `Compiled SQL <compiled-sql.html>`_ docs.
