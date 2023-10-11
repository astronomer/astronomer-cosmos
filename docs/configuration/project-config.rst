Project Config
================

The ``cosmos.config.ProjectConfig`` allows you to specify information about where your dbt project is located. It
takes the following arguments:

- ``dbt_project_path``: The full path to your dbt project. This directory should have a ``dbt_project.yml`` file
- ``models_relative_path``: The path to your models directory, relative to the ``dbt_project_path``. This defaults to
  ``models/``
- ``seeds_relative_path``: The path to your seeds directory, relative to the ``dbt_project_path``. This defaults to
  ``data/``
- ``snapshots_relative_path``: The path to your snapshots directory, relative to the ``dbt_project_path``. This defaults
  to ``snapshots/``
- ``manifest_path``: The absolute path to your manifests directory. This is only required if you're using Cosmos' manifest
  parsing mode
- ``project_name`` : The name of the project. If ``dbt_project_path`` is provided, the ``project_name`` defaults to the
  folder name containing ``dbt_project.yml``. If ``dbt_project_path`` is not provided, and ``manifest_path`` is provided,
  ``project_name`` is required as the name can not be inferred from ``dbt_project_path``


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
    )
