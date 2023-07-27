.. _selecting-excluding:

Selecting & Excluding
=======================

Cosmos allows you to filter to a subset of your dbt project in each ``DbtDag`` / ``DbtTaskGroup`` using the ``select`` and ``exclude`` parameters in the ``RenderConfig`` class.

The ``select`` and ``exclude`` parameters are lists, with values like the following:

- ``tag:my_tag``: include/exclude models with the tag ``my_tag``
- ``config.materialized:table``: include/exclude models with the config ``materialized: table``
- ``path:analytics/tables``: include/exclude models in the ``analytics/tables`` directory


.. note::

    If you're using the ``dbt_ls`` parsing method, these arguments are passed directly to the dbt CLI command.

    If you're using the ``dbt_manifest`` parsing method, Cosmos will filter the models in the manifest before creating the DAG. This does not directly use dbt's CLI command, but should include all metadata that dbt would include.

    If you're using the ``custom`` parsing method, Cosmos does not currently read the ``dbt_project.yml`` file. You can still select/exclude models if you're selecting on metadata defined in the model code or ``.yml`` files in the models directory.

Examples:

.. code-block:: python

    from cosmos import DbtDag, RenderConfig

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            select=["tag:my_tag"],
        )
    )

.. code-block:: python

    from cosmos import DbtDag

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            select=["config.schema:prod"],
        )
    )

.. code-block:: python

    from cosmos import DbtDag

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            select=["path:analytics/tables"],
        )
    )
