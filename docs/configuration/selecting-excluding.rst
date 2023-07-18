.. _selecting-excluding:

Selecting & Excluding
=======================

Cosmos allows you to filter by configs (e.g. ``materialized``, ``tags``) using the ``select`` and ``exclude`` parameters. If a model contains any of the configs in the ``select``, it gets included as part of the DAG/Task Group. Similarly, if a model contains any of the configs in the ``exclude``, it gets excluded from the DAG/Task Group.

The ``select`` and ``exclude`` parameters are dictionaries with the following keys:

- ``configs``: a list of configs to filter by. The configs are in the format ``key:value``. For example, ``tags:daily`` or ``materialized:table``.
- ``paths``: a list of paths to filter by. The paths are in the format ``path/to/dir``. For example, ``analytics`` or ``analytics/tables``.

.. note::
    Cosmos currently reads from (1) config calls in the model code and (2) .yml files in the models directory for tags. It does not read from the dbt_project.yml file.

Examples:

.. code-block:: python

    from cosmos import DbtDag

    jaffle_shop = DbtDag(
        # ...
        select={"configs": ["tags:daily"]},
    )

.. code-block:: python

    from cosmos import DbtDag

    jaffle_shop = DbtDag(
        # ...
        select={"configs": ["schema:prod"]},
    )

.. code-block:: python

    from cosmos import DbtDag

    jaffle_shop = DbtDag(
        # ...
        select={"paths": ["analytics/tables"]},
    )