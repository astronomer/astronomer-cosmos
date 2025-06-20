.. _selecting-excluding:

Selecting & Excluding
=======================

Cosmos allows you to filter to a subset of your dbt project in each ``DbtDag`` / ``DbtTaskGroup`` using the ``select`` and ``exclude`` parameters in the ``RenderConfig`` class.

 Since Cosmos 1.3, the ``selector`` parameter is also available in ``RenderConfig`` when using the ``LoadMode.DBT_LS`` to parse the dbt project into Airflow.


Using ``select`` and ``exclude``
--------------------------------

The ``select`` and ``exclude`` parameters are lists, with values like the following:

- ``tag:my_tag``: include/exclude models with the tag ``my_tag``
- ``config.meta.some_key:some_value``: include/exclude models with ``config.meta_some_key: some_value``
- ``config.materialized:table``: include/exclude models with the config ``materialized: table``
- ``path:analytics/tables``: include/exclude models in the ``analytics/tables`` directory. In this example, ``analytics/table`` is a relative path, but absolute paths are also supported.
- ``+node_name+1`` (graph operators): include/exclude the node with name ``node_name``, all its parents, and its first generation of children (`dbt graph selector docs <https://docs.getdbt.com/reference/node-selection/graph-operators>`_)
- ``+/path/to/model_g+`` (graph operators): include/exclude all the nodes in the absolute path ``/path/to/model_g``, their parents and children. Relative paths are also supported.
- ``+tag:nightly`` (graph operators): include/exclude all nodes that have tag ``nightly`` and their parents.
- ``+config.materialized:view`` (graph operators): include/exclude all the nodes that have the materialization ``view`` and their parents
- ``@node_name`` (@ operator): include/exclude the node with name ``node_name``, all its descendants, and all ancestors of those descendants. This is useful in CI environments where you want to build a model and all its descendants, but you need the ancestors of those descendants to exist first.
- ``tag:my_tag,+node_name`` (intersection): include/exclude ``node_name`` and its parents if they have the tag ``my_tag`` (`dbt set operator docs <https://docs.getdbt.com/reference/node-selection/set-operators>`_)
- ``['tag:first_tag', 'tag:second_tag']`` (union): include/exclude nodes that have either ``tag:first_tag`` or ``tag:second_tag``
- ``resource_type:<resource>``: include nodes with the resource type ``seed, snapshot, model, test, source``. For example, ``resource_type:source`` returns only nodes where resource_type == SOURCE
- ``exclude_resource_type:<resource>``: exclude nodes with the resource type ``analysis, exposure, metric, model, saved_query, seed, semantic_model, snapshot, source, test, unit_test``. For example, ``exclude_resource_type:source`` returns only nodes where resource_type != SOURCE
- ``source:my_source``: include/exclude nodes that have the source ``my_source`` and are of resource_type ``source``
- ``source:my_source+``: include/exclude nodes that have the source ``my_source`` and their children
- ``source:my_source.my_table``: include/exclude nodes that have the source ``my_source`` and the table ``my_table``
- ``exposure:my_exposure``: include/exclude nodes that have the exposure ``my_exposure`` and are of resource_type ``exposure``
- ``exposure:+my_exposure``: include/exclude nodes that have the exposure ``my_exposure`` and their parents

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


.. code-block:: python

    from cosmos import DbtDag, RenderConfig

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            select=["tag:include_tag1", "tag:include_tag2"],  # union
        )
    )

.. code-block:: python

    from cosmos import DbtDag, RenderConfig

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            select=["tag:include_tag1,tag:include_tag2"],  # intersection
        )
    )

.. code-block:: python

    from cosmos import DbtDag, RenderConfig

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            exclude=["node_name+"],  # node_name and its children
        )
    )

.. code-block:: python

    from cosmos import DbtDag, RenderConfig

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            select=["@my_model"],  # selects my_model, all its descendants,
            # and all ancestors needed to build those descendants
        )
    )

Using ``selector``
--------------------------------
.. note::
    Only currently supported using the ``dbt_ls`` parsing method since Cosmos 1.3 where the selector is passed directly to the dbt CLI command. \
    If  ``select`` and/or ``exclude`` are used with ``selector``, dbt will ignore the ``select`` and ``exclude`` parameters.

The ``selector`` parameter is a string that references a `dbt YAML selector <https://docs.getdbt.com/reference/node-selection/yaml-selectors>`_ already defined in a dbt project.

Examples:

.. code-block:: python

    from cosmos import DbtDag, RenderConfig, LoadMode

    jaffle_shop = DbtDag(
        render_config=RenderConfig(
            selector="my_selector",  # this selector must be defined in your dbt project
            load_method=LoadMode.DBT_LS,
        )
    )
