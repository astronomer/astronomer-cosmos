.. _operator-args-per-node:

Overriding operator arguments per dbt node (or group of nodes)
==============================================================

.. versionadded:: 1.8.0

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


More information about this feature can be found in :ref:`custom-airflow-properties`.

To learn how to customise the profile per dbt model or Cosmos task, check :ref:`profile-customise-per-node`.
