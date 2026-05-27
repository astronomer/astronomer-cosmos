.. _project-config:

Project config
==============

The ``cosmos.config.ProjectConfig`` allows you to specify information about where your dbt project is located and project
variables that should be used for rendering and execution. It takes the following arguments:

- ``dbt_project_path``: The full path to your dbt project. This directory should have a ``dbt_project.yml`` file
- ``models_relative_path``: The path to your models directory, relative to the ``dbt_project_path``. This defaults to ``models``
- ``seeds_relative_path``: The path to your seeds directory, relative to the ``dbt_project_path``. This defaults to ``seeds``
- ``snapshots_relative_path``: The path to your snapshots directory, relative to the ``dbt_project_path``. This defaults to ``snapshots``
- ``manifest_path``: The absolute path to your manifests directory. This is only required if you're using Cosmos' manifest
  parsing mode. Cosmos supports both local and remote paths for manifest parsing (e.g. S3 URL). See :ref:`parsing-methods` for more details.
- ``project_name`` : The name of the project. If ``dbt_project_path`` is provided, the ``project_name`` defaults to the
  folder name containing ``dbt_project.yml``. If ``dbt_project_path`` is not provided, and ``manifest_path`` is provided,
  ``project_name`` is required as the name can not be inferred from ``dbt_project_path``
- ``dbt_vars``: (new in v1.3) A dictionary of dbt variables for the project rendering and execution. This argument overrides variables
  defined in the dbt_project.yml file. The dictionary of variables is dumped to a yaml string and passed to dbt commands
  as the --vars argument. Variables are only supported for rendering when using ``RenderConfig.LoadMode.DBT_LS`` and
  ``RenderConfig.LoadMode.CUSTOM`` load mode. Variables using `Airflow templating <https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#templates-reference>`_
  will only be rendered at execution time, not at render time.
- ``env_vars``: (new in v1.3) A dictionary of environment variables used for rendering and execution. Rendering with
  env vars is only supported when using ``RenderConfig.LoadMode.DBT_LS`` load mode.
- ``install_dbt_deps``: (new in v1.9) Run dbt deps during DAG parsing and task execution if True (default).
  Also accepts an `Airflow Jinja-templated <https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#templates-reference>`_
  string (e.g. ``"{{ params.install_deps }}"``) that resolves to a boolean at task execution time. This is
  useful when you want to control ``dbt deps`` per DAG run (e.g. via an Airflow ``Param``). When a template
  string is provided, ``dbt deps`` runs during DAG parsing whenever a non-empty ``packages.yml`` or
  ``dependencies.yml`` is present (since templates are only rendered at task execution time); if no
  dependencies file exists, ``dbt deps`` is skipped at both parse and execution time.
- ``copy_dbt_packages``: (new in v1.10) Copy the dbt project ``dbt_packages`` instead of creating symbolic links, so Cosmos can run ``dbt deps`` incrementally.
- ``partial_parse``: (new in v1.4) If True, then attempt to use the ``partial_parse.msgpack`` if it exists. This is only used
  for the ``LoadMode.DBT_LS`` load mode, and for the ``ExecutionMode.LOCAL`` and ``ExecutionMode.VIRTUALENV``
  execution modes. Due to the way that dbt `partial parsing works <https://docs.getdbt.com/reference/parsing#known-limitations>`_, it does not work with Cosmos profile mapping classes. To benefit from this feature, users have to set the ``profiles_yml_filepath`` argument in ``ProfileConfig``.

Project config example
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from cosmos.config import ProjectConfig

    config = ProjectConfig(
        dbt_project_path="/path/to/dbt/project",
        models_relative_path="custom_models_folder",
        seeds_relative_path="custom_seeds_folder",
        snapshots_relative_path="custom_snapshots_folder",
        manifest_path="/path/to/manifests",
        env_vars={"MY_ENV_VAR": "my_env_value"},
        dbt_vars={
            "my_dbt_var": "my_value",
            "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
            "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
        },
    )

Controlling dbt deps per DAG run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``install_dbt_deps`` can be templated, which is useful when you want to decide whether to run ``dbt deps``
at task execution time — for example, only on the first run or when package dependencies change — driven
by an Airflow ``Param``.

.. code-block:: python

    from airflow.models.param import Param
    from cosmos import DbtDag, ProjectConfig

    dag = DbtDag(
        # ...
        project_config=ProjectConfig(
            dbt_project_path="/path/to/dbt/project",
            install_dbt_deps="{{ params.install_deps }}",
        ),
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
