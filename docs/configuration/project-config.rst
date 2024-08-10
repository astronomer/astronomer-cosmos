Project Config
================

The ``cosmos.config.ProjectConfig`` allows you to specify information about where your dbt project is located and project
variables that should be used for rendering and execution. It takes the following arguments:

- ``dbt_project_path``: The full path to your dbt project. This directory should have a ``dbt_project.yml`` file.
  Along with supporting local paths, starting with Cosmos 1.6.0, if you've Airflow >= 2.8.0, Cosmos also supports
  remote paths for your dbt project (e.g. S3 URL).
- ``dbt_project_conn_id``: The connection id for the Airflow connection that contains the credentials for the remote
  dbt project. This is only required if you're using a remote dbt project path.
- ``models_relative_path``: The path to your models directory, relative to the ``dbt_project_path``. This defaults to
  ``models/``
- ``seeds_relative_path``: The path to your seeds directory, relative to the ``dbt_project_path``. This defaults to
  ``data/``
- ``snapshots_relative_path``: The path to your snapshots directory, relative to the ``dbt_project_path``. This defaults
  to ``snapshots/``
- ``manifest_path``: The absolute path to your manifests directory. This is only required if you're using Cosmos' manifest
  parsing mode. Along with supporting local paths for manifest parsing, starting with Cosmos 1.6.0, if you've
  Airflow >= 2.8.0, Cosmos also supports remote paths for manifest parsing(e.g. S3 URL). See :ref:`parsing-methods` for more details.
- ``manifest_conn_id``: The connection id for the Airflow connection that contains the credentials for the remote
  manifest path. This is only required if you're using a remote manifest path.
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
- ``partial_parse``: (new in v1.4) If True, then attempt to use the ``partial_parse.msgpack`` if it exists. This is only used
  for the ``LoadMode.DBT_LS`` load mode, and for the ``ExecutionMode.LOCAL`` and ``ExecutionMode.VIRTUALENV``
  execution modes. Due to the way that dbt `partial parsing works <https://docs.getdbt.com/reference/parsing#known-limitations>`_, it does not work with Cosmos profile mapping classes. To benefit from this feature, users have to set the ``profiles_yml_filepath`` argument in ``ProfileConfig``.

Project Config Example
----------------------

.. code-block:: python

    from cosmos.config import ProjectConfig

    config = ProjectConfig(
        dbt_project_path="/path/to/dbt/project",
        models_relative_path="models",
        seeds_relative_path="data",
        snapshots_relative_path="snapshots",
        manifest_path="/path/to/manifests",
        env_vars={"MY_ENV_VAR": "my_env_value"},
        dbt_vars={
            "my_dbt_var": "my_value",
            "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
            "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
        },
    )
