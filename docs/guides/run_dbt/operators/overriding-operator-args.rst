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

While configuring in the dbt model YAML (e.g. ``models/schema.yml``) a different behaviour for the model "expensive", that should use the "expensive-pool":

.. code-block:: yaml

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

.. _operator-args-test-nodes:

Overriding operator arguments for dbt tests
-------------------------------------------

When using ``TestBehavior.AFTER_EACH``, the test task inherits the operator arguments of the resource it tests, and then
applies the ``operator_kwargs`` declared by the tests it runs. This allows, for example, retrying model runs without
retrying tests, which is useful when retrying a slow test would delay subsequent DAG runs:

.. code-block:: yaml

    # dbt_project.yml - retry every model run twice, but never retry their tests
    models:
      my_dbt_project:
        +meta:
          cosmos:
            operator_kwargs:
              retries: 2

    data_tests:  # named `tests` before dbt 1.8
      my_dbt_project:
        +meta:
          cosmos:
            operator_kwargs:
              retries: 0

This works for both dbt test types. Generic (schema) tests are configured as above, or individually in the model YAML,
while singular tests can also declare ``{{ config(meta={"cosmos": {"operator_kwargs": {...}}}) }}`` in their SQL file.

Since all the tests of a dbt node run in a single Airflow task, if they declare the same argument with different values,
the last one wins and Cosmos logs a warning. Tests without a parent resource (e.g. a singular test that does not
``ref()`` any model) are not rendered under ``TestBehavior.AFTER_EACH``, so their ``operator_kwargs`` do not apply.
